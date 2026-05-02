// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::app::TorrentMetrics;
use crate::config::Settings;
use crate::integrations::cli::{SyntheticLoadArgs, SyntheticLoadMode};
use crate::networking::protocol::{generate_message, Message};
use crate::resource_manager::{
    ResourceManager, ResourceManagerClient, ResourceManagerSnapshot, ResourceType, ResourceUsage,
};
use crate::token_bucket::TokenBucket;
use crate::torrent_file::{Info, Torrent};
use crate::torrent_manager::{ManagerCommand, ManagerEvent, TorrentManager, TorrentParameters};

use chrono::Local;
use serde::Serialize;
use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

const BLOCK_SIZE: u32 = 16_384;
const SYNTHETIC_BYTE: u8 = 0;
const MANAGER_CHANNEL_SIZE: usize = 10_000;
const EVENT_CHANNEL_SIZE: usize = 100_000;
const CLIENT_ID: &str = "SL000000000000000000";

type DynError = Box<dyn Error + Send + Sync>;

#[derive(Default)]
struct SyntheticCounters {
    download_bytes: AtomicU64,
    upload_bytes: AtomicU64,
    seeder_requests: AtomicU64,
    leecher_requests: AtomicU64,
    leecher_pieces: AtomicU64,
    connections: AtomicU64,
    disconnects: AtomicU64,
    protocol_errors: AtomicU64,
    manager_block_received: AtomicU64,
    manager_block_sent: AtomicU64,
    disk_read_started: AtomicU64,
    disk_read_finished: AtomicU64,
    disk_write_started: AtomicU64,
    disk_write_finished: AtomicU64,
}

#[derive(Clone)]
struct HarnessContext {
    event_tx: mpsc::Sender<ManagerEvent>,
    resource_client: ResourceManagerClient,
    global_dl_bucket: Arc<TokenBucket>,
    global_ul_bucket: Arc<TokenBucket>,
    counters: Arc<SyntheticCounters>,
    shutdown_tx: broadcast::Sender<()>,
}

#[derive(Clone)]
struct SyntheticTorrentSpec {
    index: usize,
    name: String,
    total_size: u64,
    piece_size: u64,
    piece_count: usize,
    info_hash: Vec<u8>,
    torrent: Torrent,
}

struct ManagerRuntime {
    command_tx: mpsc::Sender<ManagerCommand>,
    metrics_rx: watch::Receiver<TorrentMetrics>,
    handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
}

#[derive(Clone, Copy)]
struct RunTopology {
    download_peers: usize,
    upload_peers: usize,
}

#[derive(Serialize)]
struct SyntheticSample {
    elapsed_ms: u128,
    phase: &'static str,
    download_bytes_total: u64,
    upload_bytes_total: u64,
    download_bps: u64,
    upload_bps: u64,
    manager_download_bps: u64,
    manager_upload_bps: u64,
    completed_pieces: u64,
    total_pieces: u64,
    connected_peers_reported: u64,
    seeder_requests: u64,
    leecher_requests: u64,
    leecher_pieces: u64,
    connections: u64,
    disconnects: u64,
    protocol_errors: u64,
    manager_block_received: u64,
    manager_block_sent: u64,
    disk_read_started: u64,
    disk_read_finished: u64,
    disk_write_started: u64,
    disk_write_finished: u64,
    resources: ResourceSampleSet,
}

#[derive(Serialize)]
struct SyntheticSummary {
    run_id: String,
    mode: String,
    torrents: usize,
    requested_peers: usize,
    download_peers: usize,
    upload_peers: usize,
    size_per_torrent_bytes: u64,
    piece_size_bytes: u64,
    duration_secs: u64,
    warmup_secs: u64,
    measured_secs: f64,
    download_bytes: u64,
    upload_bytes: u64,
    avg_download_bps: u64,
    avg_upload_bps: u64,
    avg_download_mbps: f64,
    avg_upload_mbps: f64,
    seeder_requests: u64,
    leecher_requests: u64,
    leecher_pieces: u64,
    connections: u64,
    disconnects: u64,
    protocol_errors: u64,
    manager_block_received: u64,
    manager_block_sent: u64,
    disk_read_started: u64,
    disk_read_finished: u64,
    disk_write_started: u64,
    disk_write_finished: u64,
    output_dir: PathBuf,
}

#[derive(Default, Serialize)]
struct ResourceSampleSet {
    peer_connection: ResourceSample,
    disk_read: ResourceSample,
    disk_write: ResourceSample,
}

#[derive(Default, Serialize)]
struct ResourceSample {
    limit: usize,
    in_use: usize,
    queued: usize,
    max_queue_size: usize,
}

pub async fn run(args: &SyntheticLoadArgs, json_output: bool) -> Result<(), DynError> {
    let config = ParsedSyntheticConfig::from_args(args)?;
    let run_id = Local::now().format("run_%Y%m%d_%H%M%S").to_string();
    let output_dir = args.out.join(&run_id);
    tokio::fs::create_dir_all(&output_dir).await?;
    tokio::fs::create_dir_all(output_dir.join("data")).await?;

    let counters = Arc::new(SyntheticCounters::default());
    let (harness_shutdown_tx, _) = broadcast::channel::<()>(16);
    let (resource_shutdown_tx, _) = broadcast::channel::<()>(1);
    let topology = topology_for(args.mode, args.peers, args.torrents)?;
    let resource_manager = build_resource_manager(args, topology, resource_shutdown_tx.clone());
    let resource_client = resource_manager.1.clone();
    tokio::spawn(resource_manager.0.run());

    let (event_tx, event_rx) = mpsc::channel::<ManagerEvent>(EVENT_CHANNEL_SIZE);
    let event_handle = tokio::spawn(collect_manager_events(event_rx, counters.clone()));

    let specs = build_torrent_specs(args.torrents, config.size_per_torrent, config.piece_size)?;

    let rate_limit = args
        .target_gbps
        .map(gbps_to_bytes_per_second)
        .unwrap_or(0.0);
    let global_dl_bucket = Arc::new(TokenBucket::new(rate_limit, rate_limit));
    let global_ul_bucket = Arc::new(TokenBucket::new(rate_limit, rate_limit));
    let harness = HarnessContext {
        event_tx,
        resource_client: resource_client.clone(),
        global_dl_bucket,
        global_ul_bucket,
        counters: counters.clone(),
        shutdown_tx: harness_shutdown_tx.clone(),
    };

    let mut managers = Vec::new();
    let mut peer_handles = Vec::new();

    if topology.download_peers > 0 {
        let download_dir = output_dir.join("data").join("download");
        let download_setup =
            start_download_side(&specs, topology.download_peers, &download_dir, &harness).await?;
        managers.extend(download_setup.managers);
        peer_handles.extend(download_setup.peer_handles);
    }

    if topology.upload_peers > 0 {
        let upload_dir = output_dir.join("data").join("upload");
        let upload_setup = start_upload_side(
            &specs,
            topology.upload_peers,
            &upload_dir,
            &harness,
            args.leecher_pipeline,
        )
        .await?;
        managers.extend(upload_setup.managers);
        peer_handles.extend(upload_setup.peer_handles);
    }

    drop(harness);

    let samples_path = output_dir.join("samples.jsonl");
    let mut sample_writer = BufWriter::new(File::create(&samples_path)?);
    let summary = sample_loop(
        SampleContext {
            args,
            config: &config,
            topology,
            run_id: &run_id,
            output_dir: &output_dir,
            counters: counters.clone(),
            resource_client: &resource_client,
            managers: &managers,
            json_output,
        },
        &mut sample_writer,
    )
    .await?;
    sample_writer.flush()?;

    shutdown_managers(&mut managers).await;
    let _ = harness_shutdown_tx.send(());
    let _ = resource_shutdown_tx.send(());
    for handle in peer_handles {
        handle.abort();
    }
    event_handle.abort();

    let summary_path = output_dir.join("summary.json");
    tokio::fs::write(&summary_path, serde_json::to_vec_pretty(&summary)?).await?;

    if json_output {
        println!("{}", serde_json::to_string_pretty(&summary)?);
    } else {
        println!(
            "Synthetic load complete: down={} up={} samples={} summary={}",
            format_bps(summary.avg_download_bps),
            format_bps(summary.avg_upload_bps),
            samples_path.display(),
            summary_path.display()
        );
    }

    Ok(())
}

struct ParsedSyntheticConfig {
    size_per_torrent: u64,
    piece_size: u64,
}

impl ParsedSyntheticConfig {
    fn from_args(args: &SyntheticLoadArgs) -> Result<Self, DynError> {
        if args.torrents == 0 {
            return Err("synthetic-load requires --torrents greater than 0".into());
        }
        if args.peers == 0 {
            return Err("synthetic-load requires --peers greater than 0".into());
        }
        if args.duration_secs == 0 {
            return Err("synthetic-load requires --duration-secs greater than 0".into());
        }
        if args.metrics_interval_ms == 0 {
            return Err("synthetic-load requires --metrics-interval-ms greater than 0".into());
        }
        if args.leecher_pipeline == 0 {
            return Err("synthetic-load requires --leecher-pipeline greater than 0".into());
        }

        let size_per_torrent = parse_size(&args.size_per_torrent)?;
        let piece_size = parse_size(&args.piece_size)?;
        if size_per_torrent == 0 {
            return Err("--size-per-torrent must be greater than 0".into());
        }
        if piece_size == 0 || piece_size > u32::MAX as u64 {
            return Err("--piece-size must be between 1 byte and u32::MAX".into());
        }
        if piece_size > size_per_torrent {
            return Err("--piece-size must not exceed --size-per-torrent".into());
        }

        Ok(Self {
            size_per_torrent,
            piece_size,
        })
    }
}

struct SideSetup {
    managers: Vec<ManagerRuntime>,
    peer_handles: Vec<JoinHandle<()>>,
}

async fn start_download_side(
    specs: &[SyntheticTorrentSpec],
    peers: usize,
    data_root: &Path,
    harness: &HarnessContext,
) -> Result<SideSetup, DynError> {
    tokio::fs::create_dir_all(data_root).await?;

    let mut peer_addrs_by_torrent: Vec<Vec<SocketAddr>> = vec![Vec::new(); specs.len()];
    let mut peer_handles = Vec::new();
    for peer_index in 0..peers {
        let torrent_index = peer_index % specs.len();
        let spec = specs[torrent_index].clone();
        let (addr, handle) = spawn_synthetic_seeder(
            spec,
            peer_index,
            harness.counters.clone(),
            harness.shutdown_tx.clone(),
        )
        .await?;
        peer_addrs_by_torrent[torrent_index].push(addr);
        peer_handles.push(handle);
    }

    let mut managers = Vec::new();
    for spec in specs {
        let manager = build_manager(
            spec,
            data_root.join(format!("torrent_{:04}", spec.index)),
            false,
            harness,
        )?;
        let (mut manager, command_tx, metrics_rx) = manager;
        for addr in &peer_addrs_by_torrent[spec.index] {
            manager.connect_to_peer(*addr);
        }
        let handle = tokio::spawn(async move { manager.run(false).await });
        managers.push(ManagerRuntime {
            command_tx,
            metrics_rx,
            handle,
        });
    }

    Ok(SideSetup {
        managers,
        peer_handles,
    })
}

async fn start_upload_side(
    specs: &[SyntheticTorrentSpec],
    peers: usize,
    data_root: &Path,
    harness: &HarnessContext,
    leecher_pipeline: usize,
) -> Result<SideSetup, DynError> {
    tokio::fs::create_dir_all(data_root).await?;

    let mut managers = Vec::new();
    let mut listener_addrs = Vec::new();
    let mut peer_handles = Vec::new();

    for spec in specs {
        let torrent_dir = data_root.join(format!("torrent_{:04}", spec.index));
        prepare_seed_file(spec, &torrent_dir).await?;
        let (incoming_tx, incoming_rx) = mpsc::channel(MANAGER_CHANNEL_SIZE);
        let (addr, listener_handle) = spawn_incoming_router(
            incoming_tx,
            harness.counters.clone(),
            harness.shutdown_tx.clone(),
        )
        .await?;
        listener_addrs.push(addr);
        peer_handles.push(listener_handle);

        let manager = build_manager_with_incoming(spec, torrent_dir, true, incoming_rx, harness)?;
        let (manager, command_tx, metrics_rx) = manager;
        let handle = tokio::spawn(async move { manager.run(false).await });
        managers.push(ManagerRuntime {
            command_tx,
            metrics_rx,
            handle,
        });
    }

    for peer_index in 0..peers {
        let torrent_index = peer_index % specs.len();
        let spec = specs[torrent_index].clone();
        let addr = listener_addrs[torrent_index];
        let handle = tokio::spawn(run_synthetic_leecher(
            spec,
            peer_index,
            addr,
            leecher_pipeline,
            harness.counters.clone(),
            harness.shutdown_tx.subscribe(),
        ));
        peer_handles.push(handle);
    }

    Ok(SideSetup {
        managers,
        peer_handles,
    })
}

fn build_manager(
    spec: &SyntheticTorrentSpec,
    torrent_data_path: PathBuf,
    validated: bool,
    harness: &HarnessContext,
) -> Result<
    (
        TorrentManager,
        mpsc::Sender<ManagerCommand>,
        watch::Receiver<TorrentMetrics>,
    ),
    DynError,
> {
    let (_incoming_tx, incoming_rx) = mpsc::channel(MANAGER_CHANNEL_SIZE);
    build_manager_with_rx(spec, torrent_data_path, validated, incoming_rx, harness)
}

fn build_manager_with_incoming(
    spec: &SyntheticTorrentSpec,
    torrent_data_path: PathBuf,
    validated: bool,
    incoming_rx: mpsc::Receiver<(TcpStream, Vec<u8>)>,
    harness: &HarnessContext,
) -> Result<
    (
        TorrentManager,
        mpsc::Sender<ManagerCommand>,
        watch::Receiver<TorrentMetrics>,
    ),
    DynError,
> {
    build_manager_with_rx(spec, torrent_data_path, validated, incoming_rx, harness)
}

fn build_manager_with_rx(
    spec: &SyntheticTorrentSpec,
    torrent_data_path: PathBuf,
    validated: bool,
    incoming_rx: mpsc::Receiver<(TcpStream, Vec<u8>)>,
    harness: &HarnessContext,
) -> Result<
    (
        TorrentManager,
        mpsc::Sender<ManagerCommand>,
        watch::Receiver<TorrentMetrics>,
    ),
    DynError,
> {
    let (command_tx, command_rx) = mpsc::channel(MANAGER_CHANNEL_SIZE);
    let (metrics_tx, metrics_rx) = watch::channel(TorrentMetrics::default());
    let settings = Arc::new(Settings {
        client_id: CLIENT_ID.to_string(),
        private_client: false,
        ..Default::default()
    });
    let params = TorrentParameters {
        dht_handle: crate::dht_service::DhtHandle::disabled(),
        incoming_peer_rx: incoming_rx,
        metrics_tx,
        torrent_validation_status: validated,
        torrent_data_path: Some(torrent_data_path),
        container_name: None,
        manager_command_rx: command_rx,
        manager_event_tx: harness.event_tx.clone(),
        settings,
        resource_manager: harness.resource_client.clone(),
        global_dl_bucket: harness.global_dl_bucket.clone(),
        global_ul_bucket: harness.global_ul_bucket.clone(),
        file_priorities: HashMap::new(),
    };

    let manager = TorrentManager::from_torrent(params, spec.torrent.clone())
        .map_err(|message| format!("failed to build synthetic manager: {message}"))?;
    Ok((manager, command_tx, metrics_rx))
}

async fn spawn_synthetic_seeder(
    spec: SyntheticTorrentSpec,
    peer_index: usize,
    counters: Arc<SyntheticCounters>,
    shutdown_tx: broadcast::Sender<()>,
) -> Result<(SocketAddr, JoinHandle<()>), DynError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let mut shutdown_rx = shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _)) => {
                            counters.connections.fetch_add(1, Ordering::Relaxed);
                            let peer_id = synthetic_peer_id(b'S', peer_index);
                            let counters = counters.clone();
                            let spec = spec.clone();
                            let mut child_shutdown = shutdown_tx.subscribe();
                            tokio::spawn(async move {
                                if run_seeder_connection(stream, &spec, peer_id, counters.clone(), &mut child_shutdown).await.is_err() {
                                    counters.protocol_errors.fetch_add(1, Ordering::Relaxed);
                                }
                                counters.disconnects.fetch_add(1, Ordering::Relaxed);
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });
    Ok((addr, handle))
}

async fn run_seeder_connection(
    stream: TcpStream,
    spec: &SyntheticTorrentSpec,
    peer_id: Vec<u8>,
    counters: Arc<SyntheticCounters>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> Result<(), DynError> {
    let (mut reader, mut writer) = stream.into_split();
    let mut handshake = vec![0u8; 68];
    reader.read_exact(&mut handshake).await?;
    if handshake.get(28..48) != Some(spec.info_hash.as_slice()) {
        return Err("synthetic seeder received mismatched info hash".into());
    }

    writer
        .write_all(&generate_message(Message::Handshake(
            spec.info_hash.clone(),
            peer_id,
        ))?)
        .await?;
    writer
        .write_all(&generate_message(Message::Bitfield(full_bitfield(
            spec.piece_count,
        )))?)
        .await?;
    writer
        .write_all(&generate_message(Message::Unchoke)?)
        .await?;

    let mut socket_buf = vec![0u8; 64 * 1024];
    let mut parse_buf = Vec::with_capacity(128 * 1024);
    let mut data_block = vec![SYNTHETIC_BYTE; BLOCK_SIZE as usize];

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,
            read = reader.read(&mut socket_buf) => {
                let n = read?;
                if n == 0 {
                    break;
                }
                parse_buf.extend_from_slice(&socket_buf[..n]);
                while let Some(frame) = take_frame(&mut parse_buf) {
                    match frame_message_id(&frame) {
                        Some(2) => {
                            writer.write_all(&generate_message(Message::Unchoke)?).await?;
                        }
                        Some(6) => {
                            if let Some((index, begin, length)) = parse_request_payload(&frame) {
                                let len = length as usize;
                                if data_block.len() < len {
                                    data_block.resize(len, SYNTHETIC_BYTE);
                                }
                                write_piece_frame(&mut writer, index, begin, &data_block[..len]).await?;
                                counters.seeder_requests.fetch_add(1, Ordering::Relaxed);
                                counters.download_bytes.fetch_add(length as u64, Ordering::Relaxed);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    Ok(())
}

async fn spawn_incoming_router(
    incoming_tx: mpsc::Sender<(TcpStream, Vec<u8>)>,
    counters: Arc<SyntheticCounters>,
    shutdown_tx: broadcast::Sender<()>,
) -> Result<(SocketAddr, JoinHandle<()>), DynError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let mut shutdown_rx = shutdown_tx.subscribe();
        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => break,
                accepted = listener.accept() => {
                    let Ok((mut stream, _)) = accepted else {
                        break;
                    };
                    counters.connections.fetch_add(1, Ordering::Relaxed);
                    let tx = incoming_tx.clone();
                    let counters = counters.clone();
                    tokio::spawn(async move {
                        let mut handshake = vec![0u8; 68];
                        match stream.read_exact(&mut handshake).await {
                            Ok(_) => {
                                if tx.send((stream, handshake)).await.is_err() {
                                    counters.protocol_errors.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(_) => {
                                counters.protocol_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    });
                }
            }
        }
    });
    Ok((addr, handle))
}

async fn run_synthetic_leecher(
    spec: SyntheticTorrentSpec,
    peer_index: usize,
    addr: SocketAddr,
    pipeline_depth: usize,
    counters: Arc<SyntheticCounters>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let result = async {
        let stream = TcpStream::connect(addr).await?;
        let (mut reader, mut writer) = stream.into_split();
        writer
            .write_all(&generate_message(Message::Handshake(
                spec.info_hash.clone(),
                synthetic_peer_id(b'L', peer_index),
            ))?)
            .await?;

        let mut handshake = vec![0u8; 68];
        reader.read_exact(&mut handshake).await?;
        writer.write_all(&generate_message(Message::Interested)?).await?;

        let mut next_block = 0u64;
        let total_blocks = spec.total_size.div_ceil(BLOCK_SIZE as u64).max(1);
        let mut in_flight = 0usize;
        let mut unchoked = false;
        let mut socket_buf = vec![0u8; 64 * 1024];
        let mut parse_buf = Vec::with_capacity(256 * 1024);

        loop {
            if unchoked {
                while in_flight < pipeline_depth {
                    let (piece, begin, len) =
                        block_request_for(spec.total_size, spec.piece_size, next_block);
                    write_request_frame(&mut writer, piece, begin, len).await?;
                    counters.leecher_requests.fetch_add(1, Ordering::Relaxed);
                    in_flight += 1;
                    next_block = (next_block + 1) % total_blocks;
                }
            }

            tokio::select! {
                _ = shutdown_rx.recv() => break,
                read = reader.read(&mut socket_buf) => {
                    let n = read?;
                    if n == 0 {
                        break;
                    }
                    parse_buf.extend_from_slice(&socket_buf[..n]);
                    while let Some(frame) = take_frame(&mut parse_buf) {
                        match frame_message_id(&frame) {
                            Some(1) => {
                                unchoked = true;
                            }
                            Some(7) => {
                                if let Some(piece_len) = parse_piece_payload_len(&frame) {
                                    counters.leecher_pieces.fetch_add(1, Ordering::Relaxed);
                                    counters.upload_bytes.fetch_add(piece_len as u64, Ordering::Relaxed);
                                    in_flight = in_flight.saturating_sub(1);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        Ok::<(), DynError>(())
    }
    .await;

    if result.is_err() {
        counters.protocol_errors.fetch_add(1, Ordering::Relaxed);
    }
    counters.disconnects.fetch_add(1, Ordering::Relaxed);
}

struct SampleContext<'a> {
    args: &'a SyntheticLoadArgs,
    config: &'a ParsedSyntheticConfig,
    topology: RunTopology,
    run_id: &'a str,
    output_dir: &'a Path,
    counters: Arc<SyntheticCounters>,
    resource_client: &'a ResourceManagerClient,
    managers: &'a [ManagerRuntime],
    json_output: bool,
}

async fn sample_loop(
    context: SampleContext<'_>,
    sample_writer: &mut BufWriter<File>,
) -> Result<SyntheticSummary, DynError> {
    let SampleContext {
        args,
        config,
        topology,
        run_id,
        output_dir,
        counters,
        resource_client,
        managers,
        json_output,
    } = context;

    let warmup = Duration::from_secs(args.warmup_secs);
    let measurement = Duration::from_secs(args.duration_secs);
    let total = warmup + measurement;
    let interval = Duration::from_millis(args.metrics_interval_ms);
    let start = Instant::now();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut prev_time = start;
    let mut prev_download = counters.download_bytes.load(Ordering::Relaxed);
    let mut prev_upload = counters.upload_bytes.load(Ordering::Relaxed);
    let mut measurement_baseline: Option<(Instant, u64, u64)> = None;

    while start.elapsed() < total {
        ticker.tick().await;
        let now = Instant::now();
        let elapsed = now.duration_since(start);
        let phase = if elapsed < warmup {
            "warmup"
        } else {
            if measurement_baseline.is_none() {
                measurement_baseline = Some((
                    now,
                    counters.download_bytes.load(Ordering::Relaxed),
                    counters.upload_bytes.load(Ordering::Relaxed),
                ));
            }
            "measure"
        };

        let download_total = counters.download_bytes.load(Ordering::Relaxed);
        let upload_total = counters.upload_bytes.load(Ordering::Relaxed);
        let delta_secs = now.duration_since(prev_time).as_secs_f64().max(0.001);
        let download_bps = bytes_to_bits_per_second(download_total - prev_download, delta_secs);
        let upload_bps = bytes_to_bits_per_second(upload_total - prev_upload, delta_secs);

        let manager_totals = manager_totals(managers);
        let resources = resource_client
            .snapshot()
            .await
            .map(resource_samples)
            .unwrap_or_default();

        let sample = SyntheticSample {
            elapsed_ms: elapsed.as_millis(),
            phase,
            download_bytes_total: download_total,
            upload_bytes_total: upload_total,
            download_bps,
            upload_bps,
            manager_download_bps: manager_totals.download_bps,
            manager_upload_bps: manager_totals.upload_bps,
            completed_pieces: manager_totals.completed_pieces,
            total_pieces: manager_totals.total_pieces,
            connected_peers_reported: manager_totals.connected_peers,
            seeder_requests: counters.seeder_requests.load(Ordering::Relaxed),
            leecher_requests: counters.leecher_requests.load(Ordering::Relaxed),
            leecher_pieces: counters.leecher_pieces.load(Ordering::Relaxed),
            connections: counters.connections.load(Ordering::Relaxed),
            disconnects: counters.disconnects.load(Ordering::Relaxed),
            protocol_errors: counters.protocol_errors.load(Ordering::Relaxed),
            manager_block_received: counters.manager_block_received.load(Ordering::Relaxed),
            manager_block_sent: counters.manager_block_sent.load(Ordering::Relaxed),
            disk_read_started: counters.disk_read_started.load(Ordering::Relaxed),
            disk_read_finished: counters.disk_read_finished.load(Ordering::Relaxed),
            disk_write_started: counters.disk_write_started.load(Ordering::Relaxed),
            disk_write_finished: counters.disk_write_finished.load(Ordering::Relaxed),
            resources,
        };
        writeln!(sample_writer, "{}", serde_json::to_string(&sample)?)?;

        if !json_output {
            println!(
                "[{:>6.1}s {:>7}] down={} up={} peers={} pieces={}/{} disk_q={}/{}",
                elapsed.as_secs_f64(),
                phase,
                format_bps(download_bps),
                format_bps(upload_bps),
                sample.connected_peers_reported,
                sample.completed_pieces,
                sample.total_pieces,
                sample.resources.disk_read.queued,
                sample.resources.disk_write.queued,
            );
        }

        prev_time = now;
        prev_download = download_total;
        prev_upload = upload_total;
    }

    let (measure_start, base_download, base_upload) = measurement_baseline.unwrap_or((
        start,
        counters.download_bytes.load(Ordering::Relaxed),
        counters.upload_bytes.load(Ordering::Relaxed),
    ));
    let measured_secs = Instant::now()
        .duration_since(measure_start)
        .as_secs_f64()
        .max(0.001);
    let download_bytes = counters
        .download_bytes
        .load(Ordering::Relaxed)
        .saturating_sub(base_download);
    let upload_bytes = counters
        .upload_bytes
        .load(Ordering::Relaxed)
        .saturating_sub(base_upload);
    let avg_download_bps = bytes_to_bits_per_second(download_bytes, measured_secs);
    let avg_upload_bps = bytes_to_bits_per_second(upload_bytes, measured_secs);

    Ok(SyntheticSummary {
        run_id: run_id.to_string(),
        mode: mode_name(args.mode).to_string(),
        torrents: args.torrents,
        requested_peers: args.peers,
        download_peers: topology.download_peers,
        upload_peers: topology.upload_peers,
        size_per_torrent_bytes: config.size_per_torrent,
        piece_size_bytes: config.piece_size,
        duration_secs: args.duration_secs,
        warmup_secs: args.warmup_secs,
        measured_secs,
        download_bytes,
        upload_bytes,
        avg_download_bps,
        avg_upload_bps,
        avg_download_mbps: avg_download_bps as f64 / 1_000_000.0,
        avg_upload_mbps: avg_upload_bps as f64 / 1_000_000.0,
        seeder_requests: counters.seeder_requests.load(Ordering::Relaxed),
        leecher_requests: counters.leecher_requests.load(Ordering::Relaxed),
        leecher_pieces: counters.leecher_pieces.load(Ordering::Relaxed),
        connections: counters.connections.load(Ordering::Relaxed),
        disconnects: counters.disconnects.load(Ordering::Relaxed),
        protocol_errors: counters.protocol_errors.load(Ordering::Relaxed),
        manager_block_received: counters.manager_block_received.load(Ordering::Relaxed),
        manager_block_sent: counters.manager_block_sent.load(Ordering::Relaxed),
        disk_read_started: counters.disk_read_started.load(Ordering::Relaxed),
        disk_read_finished: counters.disk_read_finished.load(Ordering::Relaxed),
        disk_write_started: counters.disk_write_started.load(Ordering::Relaxed),
        disk_write_finished: counters.disk_write_finished.load(Ordering::Relaxed),
        output_dir: output_dir.to_path_buf(),
    })
}

async fn shutdown_managers(managers: &mut [ManagerRuntime]) {
    for manager in managers.iter() {
        let _ = manager.command_tx.send(ManagerCommand::Shutdown).await;
    }
    for manager in managers.iter_mut() {
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut manager.handle).await;
    }
}

async fn collect_manager_events(
    mut event_rx: mpsc::Receiver<ManagerEvent>,
    counters: Arc<SyntheticCounters>,
) {
    while let Some(event) = event_rx.recv().await {
        match event {
            ManagerEvent::BlockReceived { .. } => {
                counters
                    .manager_block_received
                    .fetch_add(1, Ordering::Relaxed);
            }
            ManagerEvent::BlockSent { .. } => {
                counters.manager_block_sent.fetch_add(1, Ordering::Relaxed);
            }
            ManagerEvent::DiskReadStarted { .. } => {
                counters.disk_read_started.fetch_add(1, Ordering::Relaxed);
            }
            ManagerEvent::DiskReadFinished => {
                counters.disk_read_finished.fetch_add(1, Ordering::Relaxed);
            }
            ManagerEvent::DiskWriteStarted { .. } => {
                counters.disk_write_started.fetch_add(1, Ordering::Relaxed);
            }
            ManagerEvent::DiskWriteFinished => {
                counters.disk_write_finished.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

fn topology_for(
    mode: SyntheticLoadMode,
    peers: usize,
    torrents: usize,
) -> Result<RunTopology, DynError> {
    let topology = match mode {
        SyntheticLoadMode::Download => RunTopology {
            download_peers: peers,
            upload_peers: 0,
        },
        SyntheticLoadMode::Upload => RunTopology {
            download_peers: 0,
            upload_peers: peers,
        },
        SyntheticLoadMode::Swarm => {
            if peers < 2 {
                return Err("--mode swarm requires at least 2 peers".into());
            }
            let download_peers = peers / 2;
            RunTopology {
                download_peers,
                upload_peers: peers - download_peers,
            }
        }
    };

    if topology.download_peers > 0 && topology.download_peers < torrents {
        return Err(
            "--peers must be at least --torrents for the active download side of this harness"
                .into(),
        );
    }
    if topology.upload_peers > 0 && topology.upload_peers < torrents {
        return Err(
            "--peers must be at least --torrents for the active upload side of this harness".into(),
        );
    }

    Ok(topology)
}

fn build_torrent_specs(
    torrents: usize,
    size_per_torrent: u64,
    piece_size: u64,
) -> Result<Vec<SyntheticTorrentSpec>, DynError> {
    let mut specs = Vec::with_capacity(torrents);
    for index in 0..torrents {
        let name = format!("synthetic-torrent-{index:04}.bin");
        let piece_count = size_per_torrent.div_ceil(piece_size) as usize;
        let mut pieces = Vec::with_capacity(piece_count * 20);
        for piece_index in 0..piece_count {
            let piece_start = piece_index as u64 * piece_size;
            let len = piece_size.min(size_per_torrent.saturating_sub(piece_start)) as usize;
            pieces.extend_from_slice(&Sha1::digest(vec![SYNTHETIC_BYTE; len]));
        }

        let info = Info {
            piece_length: piece_size as i64,
            pieces,
            private: None,
            files: Vec::new(),
            name: name.clone(),
            length: size_per_torrent as i64,
            md5sum: None,
            meta_version: None,
            file_tree: None,
        };
        let info_dict_bencode = serde_bencode::to_bytes(&info)?;
        let info_hash = Sha1::digest(&info_dict_bencode).to_vec();
        let torrent = Torrent {
            info_dict_bencode,
            info,
            announce: None,
            announce_list: None,
            url_list: None,
            creation_date: Some(0),
            comment: None,
            created_by: Some("superseedr synthetic load harness".to_string()),
            encoding: None,
            piece_layers: None,
        };
        specs.push(SyntheticTorrentSpec {
            index,
            name,
            total_size: size_per_torrent,
            piece_size,
            piece_count,
            info_hash,
            torrent,
        });
    }
    Ok(specs)
}

async fn prepare_seed_file(
    spec: &SyntheticTorrentSpec,
    torrent_dir: &Path,
) -> Result<(), DynError> {
    tokio::fs::create_dir_all(torrent_dir).await?;
    let path = torrent_dir.join(&spec.name);
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .await?;
    file.set_len(spec.total_size).await?;
    Ok(())
}

fn full_bitfield(piece_count: usize) -> Vec<u8> {
    let mut bitfield = vec![0u8; piece_count.div_ceil(8)];
    for piece_index in 0..piece_count {
        let byte_index = piece_index / 8;
        let bit_index = 7 - (piece_index % 8);
        bitfield[byte_index] |= 1 << bit_index;
    }
    bitfield
}

fn synthetic_peer_id(role: u8, index: usize) -> Vec<u8> {
    let mut id = [b'0'; 20];
    id[0] = role;
    let suffix = format!("{index:019}");
    id[1..].copy_from_slice(suffix.as_bytes());
    id.to_vec()
}

fn take_frame(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buffer.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(buffer[0..4].try_into().ok()?) as usize;
    if buffer.len() < 4 + len {
        return None;
    }
    Some(buffer.drain(0..4 + len).collect())
}

fn frame_message_id(frame: &[u8]) -> Option<u8> {
    if frame.len() <= 4 {
        None
    } else {
        Some(frame[4])
    }
}

fn parse_request_payload(frame: &[u8]) -> Option<(u32, u32, u32)> {
    if frame.len() != 17 || frame_message_id(frame) != Some(6) {
        return None;
    }
    let index = u32::from_be_bytes(frame[5..9].try_into().ok()?);
    let begin = u32::from_be_bytes(frame[9..13].try_into().ok()?);
    let length = u32::from_be_bytes(frame[13..17].try_into().ok()?);
    Some((index, begin, length))
}

fn parse_piece_payload_len(frame: &[u8]) -> Option<usize> {
    if frame.len() < 13 || frame_message_id(frame) != Some(7) {
        return None;
    }
    Some(frame.len() - 13)
}

async fn write_piece_frame<W>(
    writer: &mut W,
    piece: u32,
    begin: u32,
    data: &[u8],
) -> Result<(), DynError>
where
    W: AsyncWrite + Unpin,
{
    let len = (9 + data.len()) as u32;
    let mut header = [0u8; 13];
    header[0..4].copy_from_slice(&len.to_be_bytes());
    header[4] = 7;
    header[5..9].copy_from_slice(&piece.to_be_bytes());
    header[9..13].copy_from_slice(&begin.to_be_bytes());
    writer.write_all(&header).await?;
    writer.write_all(data).await?;
    Ok(())
}

async fn write_request_frame<W>(
    writer: &mut W,
    piece: u32,
    begin: u32,
    length: u32,
) -> Result<(), DynError>
where
    W: AsyncWrite + Unpin,
{
    let mut frame = [0u8; 17];
    frame[0..4].copy_from_slice(&13u32.to_be_bytes());
    frame[4] = 6;
    frame[5..9].copy_from_slice(&piece.to_be_bytes());
    frame[9..13].copy_from_slice(&begin.to_be_bytes());
    frame[13..17].copy_from_slice(&length.to_be_bytes());
    writer.write_all(&frame).await?;
    Ok(())
}

fn block_request_for(total_size: u64, piece_size: u64, block_index: u64) -> (u32, u32, u32) {
    let global_offset = (block_index * BLOCK_SIZE as u64) % total_size;
    let piece = (global_offset / piece_size) as u32;
    let begin = (global_offset % piece_size) as u32;
    let remaining_piece = piece_size - begin as u64;
    let remaining_total = total_size - global_offset;
    let len = (BLOCK_SIZE as u64)
        .min(remaining_piece)
        .min(remaining_total)
        .max(1) as u32;
    (piece, begin, len)
}

struct ManagerTotals {
    download_bps: u64,
    upload_bps: u64,
    completed_pieces: u64,
    total_pieces: u64,
    connected_peers: u64,
}

fn manager_totals(managers: &[ManagerRuntime]) -> ManagerTotals {
    let mut totals = ManagerTotals {
        download_bps: 0,
        upload_bps: 0,
        completed_pieces: 0,
        total_pieces: 0,
        connected_peers: 0,
    };
    for manager in managers {
        let metrics = manager.metrics_rx.borrow();
        totals.download_bps = totals
            .download_bps
            .saturating_add(metrics.download_speed_bps);
        totals.upload_bps = totals.upload_bps.saturating_add(metrics.upload_speed_bps);
        totals.completed_pieces = totals
            .completed_pieces
            .saturating_add(metrics.number_of_pieces_completed as u64);
        totals.total_pieces = totals
            .total_pieces
            .saturating_add(metrics.number_of_pieces_total as u64);
        totals.connected_peers = totals
            .connected_peers
            .saturating_add(metrics.number_of_successfully_connected_peers as u64);
    }
    totals
}

fn resource_samples(snapshot: ResourceManagerSnapshot) -> ResourceSampleSet {
    ResourceSampleSet {
        peer_connection: resource_sample(snapshot.resources.get(&ResourceType::PeerConnection)),
        disk_read: resource_sample(snapshot.resources.get(&ResourceType::DiskRead)),
        disk_write: resource_sample(snapshot.resources.get(&ResourceType::DiskWrite)),
    }
}

fn resource_sample(usage: Option<&ResourceUsage>) -> ResourceSample {
    usage
        .map(|usage| ResourceSample {
            limit: usage.limit,
            in_use: usage.in_use,
            queued: usage.queued,
            max_queue_size: usage.max_queue_size,
        })
        .unwrap_or_default()
}

fn build_resource_manager_limits(
    args: &SyntheticLoadArgs,
    topology: RunTopology,
) -> HashMap<ResourceType, (usize, usize)> {
    let active_peers = topology.download_peers + topology.upload_peers;
    let peer_limit = args
        .peer_connection_permits
        .unwrap_or_else(|| active_peers.saturating_mul(2).saturating_add(128).max(256));
    let mut limits = HashMap::new();
    limits.insert(ResourceType::Reserve, (0, 0));
    limits.insert(ResourceType::PeerConnection, (peer_limit, peer_limit * 2));
    limits.insert(
        ResourceType::DiskRead,
        (args.disk_read_permits, args.disk_read_permits * 4),
    );
    limits.insert(
        ResourceType::DiskWrite,
        (args.disk_write_permits, args.disk_write_permits * 4),
    );
    limits
}

fn parse_size(raw: &str) -> Result<u64, DynError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("size value must not be empty".into());
    }
    let split_at = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    let number: f64 = trimmed[..split_at].parse()?;
    let unit = trimmed[split_at..].trim().to_ascii_lowercase();
    let multiplier = match unit.as_str() {
        "" | "b" => 1.0,
        "k" | "kb" => 1_000.0,
        "m" | "mb" => 1_000_000.0,
        "g" | "gb" => 1_000_000_000.0,
        "t" | "tb" => 1_000_000_000_000.0,
        "ki" | "kib" => 1024.0,
        "mi" | "mib" => 1024.0 * 1024.0,
        "gi" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "ti" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return Err(format!("unsupported size unit in '{raw}'").into()),
    };
    let bytes = number * multiplier;
    if !bytes.is_finite() || bytes < 0.0 || bytes > u64::MAX as f64 {
        return Err(format!("invalid size value '{raw}'").into());
    }
    Ok(bytes.round() as u64)
}

fn gbps_to_bytes_per_second(gbps: f64) -> f64 {
    if gbps <= 0.0 || !gbps.is_finite() {
        0.0
    } else {
        gbps * 1_000_000_000.0 / 8.0
    }
}

fn bytes_to_bits_per_second(bytes: u64, secs: f64) -> u64 {
    ((bytes as f64 * 8.0) / secs.max(0.001)) as u64
}

fn format_bps(bits_per_second: u64) -> String {
    let bps = bits_per_second as f64;
    if bps >= 1_000_000_000.0 {
        format!("{:.2}Gbps", bps / 1_000_000_000.0)
    } else if bps >= 1_000_000.0 {
        format!("{:.1}Mbps", bps / 1_000_000.0)
    } else if bps >= 1_000.0 {
        format!("{:.1}Kbps", bps / 1_000.0)
    } else {
        format!("{}bps", bits_per_second)
    }
}

fn mode_name(mode: SyntheticLoadMode) -> &'static str {
    match mode {
        SyntheticLoadMode::Download => "download",
        SyntheticLoadMode::Upload => "upload",
        SyntheticLoadMode::Swarm => "swarm",
    }
}

fn build_resource_manager(
    args: &SyntheticLoadArgs,
    topology: RunTopology,
    shutdown_tx: broadcast::Sender<()>,
) -> (ResourceManager, ResourceManagerClient) {
    ResourceManager::new(build_resource_manager_limits(args, topology), shutdown_tx)
}
