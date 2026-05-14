// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};

use tokio::{
    io::{self as tokio_io, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream},
    net::UdpSocket,
    time::{self, Instant},
};

use crate::networking::transport::{PeerConnection, PeerConnectionDirection, PeerEndpoint};

const HEADER_LEN: usize = 20;
const UTP_VERSION: u8 = 1;
const TYPE_DATA: u8 = 0;
const TYPE_FIN: u8 = 1;
const TYPE_STATE: u8 = 2;
const TYPE_RESET: u8 = 3;
const TYPE_SYN: u8 = 4;
const EXT_NONE: u8 = 0;
const EXT_SELECTIVE_ACK: u8 = 1;

const RECEIVE_WINDOW: u32 = 1_048_576;
const MIN_PACKET_SIZE: usize = 150;
const MAX_PACKET_SIZE: usize = 1_200;
const STREAM_BUFFER: usize = 256 * 1024;
const MAX_INFLIGHT_PACKETS: usize = 64;
const CONNECT_RETRIES: usize = 4;
const CONNECT_RETRY_TIMEOUT: Duration = Duration::from_millis(400);
const INITIAL_RETRANSMIT_TIMEOUT: Duration = Duration::from_secs(1);
const MIN_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(500);
const RETRANSMIT_TICK: Duration = Duration::from_millis(100);
const MAX_RETRANSMITS: u8 = 8;
const DELAY_TARGET_MICROSECONDS: u32 = 100_000;
const BASE_DELAY_WINDOW: Duration = Duration::from_secs(120);
const BASE_DELAY_BUCKET: Duration = Duration::from_secs(1);
const MAX_CWND_INCREASE_BYTES_PER_RTT: f64 = 3_000.0;
const LOSS_WINDOW_FACTOR: f64 = 0.5;
const SACK_EXTENSION_BYTES: usize = 4;
const MAX_OUT_OF_ORDER_PACKETS: usize = 256;
const UTP_CONNECT_LOG_ENV: &str = "SUPERSEEDR_LOG_UTP_CONNECT";

/// Homegrown BEP 29/uTP outbound transport.
///
/// This supports outbound stream-like connections with packet reliability,
/// selective ACK handling, adaptive retransmit timeouts, and LEDBAT-style
/// delay-based congestion control. Inbound UDP demultiplexing is still left for a
/// later transport-runtime pass.
pub struct UtpPeerTransport;

impl UtpPeerTransport {
    pub async fn connect(remote_addr: SocketAddr) -> io::Result<PeerConnection> {
        let socket = bind_outbound_socket(remote_addr).await?;
        socket.connect(remote_addr).await?;

        let start = Instant::now();
        let receive_connection_id = random_connection_id();
        let send_connection_id = receive_connection_id.wrapping_add(1);
        let initial_seq_nr = 1;

        let syn = UtpPacket {
            packet_type: TYPE_SYN,
            connection_id: receive_connection_id,
            timestamp_microseconds: timestamp_microseconds(start),
            timestamp_difference_microseconds: 0,
            wnd_size: RECEIVE_WINDOW,
            seq_nr: initial_seq_nr,
            ack_nr: 0,
            selective_ack: Vec::new(),
            payload: Vec::new(),
        };
        let syn_bytes = syn.encode();
        let mut buf = vec![0_u8; 2_048];

        let state = loop {
            socket.send(&syn_bytes).await?;

            match time::timeout(CONNECT_RETRY_TIMEOUT, socket.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    let packet = UtpPacket::decode(&buf[..n])?;
                    if packet.packet_type == TYPE_RESET {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "uTP peer reset during connect",
                        ));
                    }
                    if packet.packet_type == TYPE_STATE
                        && packet.connection_id == receive_connection_id
                        && packet.ack_nr == initial_seq_nr
                    {
                        break UtpDriverState {
                            send_connection_id,
                            receive_connection_id,
                            next_send_seq_nr: initial_seq_nr.wrapping_add(1),
                            last_remote_seq_nr: packet.seq_nr.wrapping_sub(1),
                            reply_delay_microseconds: timestamp_microseconds(start)
                                .wrapping_sub(packet.timestamp_microseconds),
                            remote_window_bytes: packet.wnd_size as usize,
                            max_window_bytes: MAX_PACKET_SIZE as f64,
                            packet_size: MAX_PACKET_SIZE,
                            rtt_microseconds: None,
                            rtt_var_microseconds: 0.0,
                            retransmit_timeout: INITIAL_RETRANSMIT_TIMEOUT,
                            consecutive_timeouts: 0,
                            last_ack_nr_seen: packet.ack_nr,
                            duplicate_ack_count: 0,
                            delay_history: DelayHistory::default(),
                            start,
                        };
                    }
                }
                Ok(Err(error)) => return Err(error),
                Err(_) => {}
            }

            if elapsed_connect_attempts(start) >= CONNECT_RETRIES {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "uTP connect timed out",
                ));
            }
        };

        let (client_stream, driver_stream) = tokio_io::duplex(STREAM_BUFFER);
        tokio::spawn(async move {
            if let Err(error) = run_utp_driver(socket, driver_stream, state).await {
                if utp_transport_log_enabled() {
                    tracing::info!(%remote_addr, %error, "uTP driver stopped");
                }
                tracing::debug!(%remote_addr, %error, "uTP driver stopped");
            }
        });

        Ok(PeerConnection::new(
            client_stream,
            PeerEndpoint::utp(remote_addr),
            remote_addr,
            PeerConnectionDirection::Outgoing,
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UtpPacket {
    packet_type: u8,
    connection_id: u16,
    timestamp_microseconds: u32,
    timestamp_difference_microseconds: u32,
    wnd_size: u32,
    seq_nr: u16,
    ack_nr: u16,
    selective_ack: Vec<u8>,
    payload: Vec<u8>,
}

impl UtpPacket {
    fn encode(&self) -> Vec<u8> {
        let extension_len = if self.selective_ack.is_empty() {
            0
        } else {
            2 + self.selective_ack.len()
        };
        let mut bytes = Vec::with_capacity(HEADER_LEN + extension_len + self.payload.len());
        bytes.push((self.packet_type << 4) | UTP_VERSION);
        bytes.push(if self.selective_ack.is_empty() {
            EXT_NONE
        } else {
            EXT_SELECTIVE_ACK
        });
        bytes.extend_from_slice(&self.connection_id.to_be_bytes());
        bytes.extend_from_slice(&self.timestamp_microseconds.to_be_bytes());
        bytes.extend_from_slice(&self.timestamp_difference_microseconds.to_be_bytes());
        bytes.extend_from_slice(&self.wnd_size.to_be_bytes());
        bytes.extend_from_slice(&self.seq_nr.to_be_bytes());
        bytes.extend_from_slice(&self.ack_nr.to_be_bytes());
        if !self.selective_ack.is_empty() {
            bytes.push(EXT_NONE);
            bytes.push(self.selective_ack.len() as u8);
            bytes.extend_from_slice(&self.selective_ack);
        }
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "uTP packet shorter than header",
            ));
        }

        let packet_type = bytes[0] >> 4;
        let version = bytes[0] & 0x0f;
        if version != UTP_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported uTP packet version",
            ));
        }
        if packet_type > TYPE_SYN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported uTP packet type",
            ));
        }
        let (payload_offset, selective_ack) = parse_extension_chain(bytes, bytes[1])?;

        Ok(Self {
            packet_type,
            connection_id: u16::from_be_bytes([bytes[2], bytes[3]]),
            timestamp_microseconds: u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            timestamp_difference_microseconds: u32::from_be_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11],
            ]),
            wnd_size: u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            seq_nr: u16::from_be_bytes([bytes[16], bytes[17]]),
            ack_nr: u16::from_be_bytes([bytes[18], bytes[19]]),
            selective_ack,
            payload: bytes[payload_offset..].to_vec(),
        })
    }
}

fn parse_extension_chain(bytes: &[u8], first_extension: u8) -> io::Result<(usize, Vec<u8>)> {
    let mut extension = first_extension;
    let mut offset = HEADER_LEN;
    let mut selective_ack = Vec::new();

    while extension != EXT_NONE {
        if bytes.len() < offset + 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "uTP extension header truncated",
            ));
        }

        let next_extension = bytes[offset];
        let extension_len = bytes[offset + 1] as usize;
        offset += 2;

        if bytes.len() < offset + extension_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "uTP extension body truncated",
            ));
        }

        if extension == EXT_SELECTIVE_ACK {
            if extension_len < SACK_EXTENSION_BYTES || extension_len % SACK_EXTENSION_BYTES != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "uTP selective ack extension has invalid length",
                ));
            }
            selective_ack = bytes[offset..offset + extension_len].to_vec();
        }

        offset += extension_len;
        extension = next_extension;
    }

    Ok((offset, selective_ack))
}

struct UtpDriverState {
    send_connection_id: u16,
    receive_connection_id: u16,
    next_send_seq_nr: u16,
    last_remote_seq_nr: u16,
    reply_delay_microseconds: u32,
    remote_window_bytes: usize,
    max_window_bytes: f64,
    packet_size: usize,
    rtt_microseconds: Option<f64>,
    rtt_var_microseconds: f64,
    retransmit_timeout: Duration,
    consecutive_timeouts: u8,
    last_ack_nr_seen: u16,
    duplicate_ack_count: u8,
    delay_history: DelayHistory,
    start: Instant,
}

#[derive(Clone)]
struct SentPacket {
    packet_type: u8,
    seq_nr: u16,
    payload: Vec<u8>,
    sent_at: Instant,
    retransmits: u8,
}

#[derive(Debug)]
struct AckedPacket {
    payload_len: usize,
    rtt_sample: Option<Duration>,
}

#[derive(Default)]
struct AckOutcome {
    acked_packets: Vec<AckedPacket>,
    fast_retransmit: Vec<u16>,
    advanced_ack: bool,
}

#[derive(Default)]
struct DelayHistory {
    buckets: VecDeque<DelayBucket>,
}

struct DelayBucket {
    started_at: Instant,
    min_delay_microseconds: u32,
}

impl UtpDriverState {
    fn record_received_packet(&mut self, packet: &UtpPacket) {
        let now = timestamp_microseconds(self.start);
        self.reply_delay_microseconds = now.wrapping_sub(packet.timestamp_microseconds);
        self.remote_window_bytes = packet.wnd_size as usize;
    }

    fn send_window_bytes(&self) -> usize {
        let local_window = self.max_window_bytes.max(MIN_PACKET_SIZE as f64) as usize;
        local_window.min(self.remote_window_bytes)
    }

    async fn apply_ack_outcome(
        &mut self,
        socket: &UdpSocket,
        unacked_packets: &mut VecDeque<SentPacket>,
        outcome: AckOutcome,
        packet: &UtpPacket,
    ) -> io::Result<()> {
        if outcome.advanced_ack {
            self.duplicate_ack_count = 0;
            self.last_ack_nr_seen = packet.ack_nr;
        } else if packet.ack_nr == self.last_ack_nr_seen {
            self.duplicate_ack_count = self.duplicate_ack_count.saturating_add(1);
            if self.duplicate_ack_count >= 3 {
                retransmit_packet(socket, self, unacked_packets, packet.ack_nr.wrapping_add(1))
                    .await?;
                self.on_packet_loss();
                self.duplicate_ack_count = 0;
            }
        } else {
            self.duplicate_ack_count = 0;
            self.last_ack_nr_seen = packet.ack_nr;
        }

        if !outcome.fast_retransmit.is_empty() {
            self.on_packet_loss();
            for seq_nr in outcome.fast_retransmit {
                retransmit_packet(socket, self, unacked_packets, seq_nr).await?;
            }
        }

        let acked_payload_bytes: usize = outcome
            .acked_packets
            .iter()
            .map(|packet| packet.payload_len)
            .sum();
        for acked in &outcome.acked_packets {
            if let Some(sample) = acked.rtt_sample {
                self.update_rtt(sample);
            }
        }
        if acked_payload_bytes > 0 {
            self.update_congestion_window(
                packet.timestamp_difference_microseconds,
                acked_payload_bytes,
            );
        }

        Ok(())
    }

    fn update_rtt(&mut self, sample: Duration) {
        let sample_us = sample.as_micros() as f64;
        match self.rtt_microseconds {
            Some(rtt) => {
                let delta = rtt - sample_us;
                self.rtt_var_microseconds += (delta.abs() - self.rtt_var_microseconds) / 4.0;
                self.rtt_microseconds = Some(rtt + (sample_us - rtt) / 8.0);
            }
            None => {
                self.rtt_microseconds = Some(sample_us);
                self.rtt_var_microseconds = sample_us / 2.0;
            }
        }

        let rtt = self.rtt_microseconds.unwrap_or(sample_us);
        let timeout_us =
            (rtt + self.rtt_var_microseconds * 4.0).max(MIN_RETRANSMIT_TIMEOUT.as_micros() as f64);
        self.retransmit_timeout = Duration::from_micros(timeout_us as u64);
        self.consecutive_timeouts = 0;
    }

    fn update_congestion_window(
        &mut self,
        delay_sample_microseconds: u32,
        acked_payload_bytes: usize,
    ) {
        let now = Instant::now();
        self.delay_history.record(now, delay_sample_microseconds);
        let Some(base_delay) = self.delay_history.base_delay() else {
            return;
        };

        let our_delay = delay_sample_microseconds.saturating_sub(base_delay);
        let off_target = DELAY_TARGET_MICROSECONDS as f64 - our_delay as f64;
        let delay_factor = off_target / DELAY_TARGET_MICROSECONDS as f64;
        let window_factor =
            acked_payload_bytes as f64 / self.max_window_bytes.max(acked_payload_bytes as f64);
        let scaled_gain = MAX_CWND_INCREASE_BYTES_PER_RTT * delay_factor * window_factor;
        self.max_window_bytes = (self.max_window_bytes + scaled_gain).max(0.0);

        self.packet_size = if our_delay > DELAY_TARGET_MICROSECONDS {
            MIN_PACKET_SIZE
        } else {
            MAX_PACKET_SIZE
        };
    }

    fn on_packet_loss(&mut self) {
        self.max_window_bytes =
            (self.max_window_bytes * LOSS_WINDOW_FACTOR).max(MIN_PACKET_SIZE as f64);
        self.packet_size = MIN_PACKET_SIZE;
    }
}

impl DelayHistory {
    fn record(&mut self, now: Instant, delay_microseconds: u32) {
        match self.buckets.back_mut() {
            Some(bucket) if now.duration_since(bucket.started_at) < BASE_DELAY_BUCKET => {
                bucket.min_delay_microseconds =
                    bucket.min_delay_microseconds.min(delay_microseconds);
            }
            _ => self.buckets.push_back(DelayBucket {
                started_at: now,
                min_delay_microseconds: delay_microseconds,
            }),
        }

        while self
            .buckets
            .front()
            .is_some_and(|bucket| now.duration_since(bucket.started_at) > BASE_DELAY_WINDOW)
        {
            self.buckets.pop_front();
        }
    }

    fn base_delay(&self) -> Option<u32> {
        self.buckets
            .iter()
            .map(|bucket| bucket.min_delay_microseconds)
            .min()
    }
}

async fn run_utp_driver(
    socket: UdpSocket,
    local_stream: DuplexStream,
    mut state: UtpDriverState,
) -> io::Result<()> {
    let (mut local_reader, mut local_writer) = tokio_io::split(local_stream);
    let mut udp_buf = vec![0_u8; 2_048];
    let mut local_buf = vec![0_u8; MAX_PACKET_SIZE];
    let mut pending_payloads: VecDeque<Vec<u8>> = VecDeque::new();
    let mut unacked_packets: VecDeque<SentPacket> = VecDeque::new();
    let mut out_of_order_payloads: BTreeMap<u16, Vec<u8>> = BTreeMap::new();
    let mut local_eof = false;
    let mut retransmit_tick = time::interval(RETRANSMIT_TICK);

    loop {
        flush_pending_payloads(
            &socket,
            &mut state,
            &mut pending_payloads,
            &mut unacked_packets,
        )
        .await?;

        if local_eof && pending_payloads.is_empty() && unacked_packets.is_empty() {
            send_control_packet(&socket, &mut state, TYPE_FIN).await?;
            return Ok(());
        }

        tokio::select! {
            read_result = local_reader.read(&mut local_buf), if !local_eof && pending_payloads.len() < MAX_INFLIGHT_PACKETS => {
                let bytes_read = read_result?;
                if bytes_read == 0 {
                    local_eof = true;
                } else {
                    pending_payloads.push_back(local_buf[..bytes_read].to_vec());
                }
            }
            recv_result = socket.recv(&mut udp_buf) => {
                let bytes_read = recv_result?;
                let packet = UtpPacket::decode(&udp_buf[..bytes_read])?;
                process_incoming_packet(
                    &socket,
                    &mut local_writer,
                    &mut state,
                    &mut unacked_packets,
                    &mut out_of_order_payloads,
                    packet,
                ).await?;
            }
            _ = retransmit_tick.tick() => {
                retransmit_due_packets(&socket, &mut state, &mut unacked_packets).await?;
            }
        }
    }
}

async fn flush_pending_payloads(
    socket: &UdpSocket,
    state: &mut UtpDriverState,
    pending_payloads: &mut VecDeque<Vec<u8>>,
    unacked_packets: &mut VecDeque<SentPacket>,
) -> io::Result<()> {
    while pending_payloads.front().is_some() {
        if unacked_packets.len() >= MAX_INFLIGHT_PACKETS {
            break;
        }

        let current_window = unacked_payload_bytes(unacked_packets);
        let send_window = state.send_window_bytes();
        let next_payload_len = pending_payloads
            .front()
            .map(|payload| payload.len().min(state.packet_size))
            .unwrap_or(0);
        if current_window >= send_window
            || current_window.saturating_add(next_payload_len) > send_window
        {
            break;
        }

        let payload = pop_next_payload_chunk(pending_payloads, state.packet_size);
        let seq_nr = state.next_send_seq_nr;
        state.next_send_seq_nr = state.next_send_seq_nr.wrapping_add(1);
        let packet = data_packet(state, seq_nr, payload.clone());
        socket.send(&packet.encode()).await?;
        unacked_packets.push_back(SentPacket {
            packet_type: TYPE_DATA,
            seq_nr,
            payload,
            sent_at: Instant::now(),
            retransmits: 0,
        });
    }

    Ok(())
}

async fn process_incoming_packet<W>(
    socket: &UdpSocket,
    local_writer: &mut W,
    state: &mut UtpDriverState,
    unacked_packets: &mut VecDeque<SentPacket>,
    out_of_order_payloads: &mut BTreeMap<u16, Vec<u8>>,
    packet: UtpPacket,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if packet.connection_id != state.receive_connection_id {
        return Ok(());
    }

    state.record_received_packet(&packet);

    match packet.packet_type {
        TYPE_STATE => {
            let outcome =
                acknowledge_packets(unacked_packets, packet.ack_nr, &packet.selective_ack);
            state
                .apply_ack_outcome(socket, unacked_packets, outcome, &packet)
                .await?;
        }
        TYPE_DATA => {
            let expected_seq_nr = state.last_remote_seq_nr.wrapping_add(1);
            if packet.seq_nr == expected_seq_nr {
                if !packet.payload.is_empty() {
                    local_writer.write_all(&packet.payload).await?;
                }
                state.last_remote_seq_nr = packet.seq_nr;
                deliver_buffered_payloads(local_writer, state, out_of_order_payloads).await?;
            } else if seq_gt(packet.seq_nr, expected_seq_nr)
                && out_of_order_payloads.len() < MAX_OUT_OF_ORDER_PACKETS
            {
                out_of_order_payloads
                    .entry(packet.seq_nr)
                    .or_insert(packet.payload);
            }
            send_state_packet(socket, state, out_of_order_payloads).await?;
        }
        TYPE_FIN => {
            let expected_seq_nr = state.last_remote_seq_nr.wrapping_add(1);
            if packet.seq_nr == expected_seq_nr {
                state.last_remote_seq_nr = packet.seq_nr;
            }
            send_state_packet(socket, state, out_of_order_payloads).await?;
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "uTP peer closed stream",
            ));
        }
        TYPE_RESET => {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "uTP peer reset stream",
            ));
        }
        TYPE_SYN => {}
        _ => {}
    }

    Ok(())
}

async fn retransmit_due_packets(
    socket: &UdpSocket,
    state: &mut UtpDriverState,
    unacked_packets: &mut VecDeque<SentPacket>,
) -> io::Result<()> {
    let now = Instant::now();
    let timeout = state.retransmit_timeout;
    let mut saw_timeout = false;
    for sent in unacked_packets.iter_mut() {
        if now.duration_since(sent.sent_at) < timeout {
            continue;
        }
        if sent.retransmits >= MAX_RETRANSMITS {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "uTP retransmit budget exhausted",
            ));
        }

        saw_timeout = true;
        sent.sent_at = now;
        sent.retransmits = sent.retransmits.saturating_add(1);
        let packet = UtpPacket {
            packet_type: sent.packet_type,
            connection_id: state.send_connection_id,
            timestamp_microseconds: timestamp_microseconds(state.start),
            timestamp_difference_microseconds: timestamp_difference_microseconds(state),
            wnd_size: RECEIVE_WINDOW,
            seq_nr: sent.seq_nr,
            ack_nr: state.last_remote_seq_nr,
            selective_ack: Vec::new(),
            payload: sent.payload.clone(),
        };
        socket.send(&packet.encode()).await?;
    }

    if saw_timeout {
        state.on_packet_loss();
        state.consecutive_timeouts = state.consecutive_timeouts.saturating_add(1);
        state.retransmit_timeout = doubled_duration(state.retransmit_timeout);
    }

    Ok(())
}

async fn send_control_packet(
    socket: &UdpSocket,
    state: &mut UtpDriverState,
    packet_type: u8,
) -> io::Result<()> {
    let seq_nr = state.next_send_seq_nr;
    state.next_send_seq_nr = state.next_send_seq_nr.wrapping_add(1);
    let packet = UtpPacket {
        packet_type,
        connection_id: state.send_connection_id,
        timestamp_microseconds: timestamp_microseconds(state.start),
        timestamp_difference_microseconds: timestamp_difference_microseconds(state),
        wnd_size: RECEIVE_WINDOW,
        seq_nr,
        ack_nr: state.last_remote_seq_nr,
        selective_ack: Vec::new(),
        payload: Vec::new(),
    };
    socket.send(&packet.encode()).await?;
    Ok(())
}

async fn send_state_packet(
    socket: &UdpSocket,
    state: &UtpDriverState,
    out_of_order_payloads: &BTreeMap<u16, Vec<u8>>,
) -> io::Result<()> {
    let packet = UtpPacket {
        packet_type: TYPE_STATE,
        connection_id: state.send_connection_id,
        timestamp_microseconds: timestamp_microseconds(state.start),
        timestamp_difference_microseconds: timestamp_difference_microseconds(state),
        wnd_size: advertised_window(out_of_order_payloads),
        seq_nr: state.next_send_seq_nr.wrapping_sub(1),
        ack_nr: state.last_remote_seq_nr,
        selective_ack: selective_ack_for(state.last_remote_seq_nr, out_of_order_payloads),
        payload: Vec::new(),
    };
    socket.send(&packet.encode()).await?;
    Ok(())
}

fn data_packet(state: &UtpDriverState, seq_nr: u16, payload: Vec<u8>) -> UtpPacket {
    UtpPacket {
        packet_type: TYPE_DATA,
        connection_id: state.send_connection_id,
        timestamp_microseconds: timestamp_microseconds(state.start),
        timestamp_difference_microseconds: timestamp_difference_microseconds(state),
        wnd_size: RECEIVE_WINDOW,
        seq_nr,
        ack_nr: state.last_remote_seq_nr,
        selective_ack: Vec::new(),
        payload,
    }
}

fn acknowledge_packets(
    unacked_packets: &mut VecDeque<SentPacket>,
    ack_nr: u16,
    selective_ack: &[u8],
) -> AckOutcome {
    let now = Instant::now();
    let mut outcome = AckOutcome {
        advanced_ack: unacked_packets
            .iter()
            .any(|sent| seq_lte(sent.seq_nr, ack_nr)),
        ..AckOutcome::default()
    };
    let acked_sequences: HashSet<u16> = unacked_packets
        .iter()
        .filter(|sent| {
            seq_lte(sent.seq_nr, ack_nr)
                || selective_ack_contains(selective_ack, ack_nr, sent.seq_nr)
        })
        .map(|sent| sent.seq_nr)
        .collect();

    for sent in unacked_packets.iter() {
        if acked_sequences.contains(&sent.seq_nr) {
            continue;
        }

        let later_acked = unacked_packets
            .iter()
            .filter(|candidate| {
                seq_gt(candidate.seq_nr, sent.seq_nr) && acked_sequences.contains(&candidate.seq_nr)
            })
            .take(3)
            .count();
        if later_acked >= 3 {
            outcome.fast_retransmit.push(sent.seq_nr);
        }
    }

    let mut retained = VecDeque::with_capacity(unacked_packets.len());
    while let Some(sent) = unacked_packets.pop_front() {
        if acked_sequences.contains(&sent.seq_nr) {
            outcome.acked_packets.push(AckedPacket {
                payload_len: sent.payload.len(),
                rtt_sample: (sent.retransmits == 0).then(|| now.duration_since(sent.sent_at)),
            });
        } else {
            retained.push_back(sent);
        }
    }
    *unacked_packets = retained;

    outcome
}

fn pop_next_payload_chunk(pending_payloads: &mut VecDeque<Vec<u8>>, packet_size: usize) -> Vec<u8> {
    let packet_size = packet_size.clamp(MIN_PACKET_SIZE, MAX_PACKET_SIZE);
    let front = pending_payloads
        .front_mut()
        .expect("front checked before chunk extraction");
    if front.len() <= packet_size {
        return pending_payloads.pop_front().expect("front checked above");
    }

    front.drain(..packet_size).collect()
}

async fn deliver_buffered_payloads<W>(
    local_writer: &mut W,
    state: &mut UtpDriverState,
    out_of_order_payloads: &mut BTreeMap<u16, Vec<u8>>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    loop {
        let expected_seq_nr = state.last_remote_seq_nr.wrapping_add(1);
        let Some(payload) = out_of_order_payloads.remove(&expected_seq_nr) else {
            break;
        };
        if !payload.is_empty() {
            local_writer.write_all(&payload).await?;
        }
        state.last_remote_seq_nr = expected_seq_nr;
    }

    Ok(())
}

async fn retransmit_packet(
    socket: &UdpSocket,
    state: &UtpDriverState,
    unacked_packets: &mut VecDeque<SentPacket>,
    seq_nr: u16,
) -> io::Result<()> {
    let Some(sent) = unacked_packets
        .iter_mut()
        .find(|packet| packet.seq_nr == seq_nr)
    else {
        return Ok(());
    };

    if sent.retransmits >= MAX_RETRANSMITS {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "uTP retransmit budget exhausted",
        ));
    }

    sent.sent_at = Instant::now();
    sent.retransmits = sent.retransmits.saturating_add(1);
    let packet = UtpPacket {
        packet_type: sent.packet_type,
        connection_id: state.send_connection_id,
        timestamp_microseconds: timestamp_microseconds(state.start),
        timestamp_difference_microseconds: timestamp_difference_microseconds(state),
        wnd_size: RECEIVE_WINDOW,
        seq_nr: sent.seq_nr,
        ack_nr: state.last_remote_seq_nr,
        selective_ack: Vec::new(),
        payload: sent.payload.clone(),
    };
    socket.send(&packet.encode()).await?;
    Ok(())
}

fn advertised_window(out_of_order_payloads: &BTreeMap<u16, Vec<u8>>) -> u32 {
    RECEIVE_WINDOW.saturating_sub(
        out_of_order_payloads
            .values()
            .map(|payload| payload.len() as u32)
            .sum::<u32>(),
    )
}

fn selective_ack_for(ack_nr: u16, out_of_order_payloads: &BTreeMap<u16, Vec<u8>>) -> Vec<u8> {
    if out_of_order_payloads.is_empty() {
        return Vec::new();
    }

    let mut mask = vec![0_u8; SACK_EXTENSION_BYTES];
    let mut any = false;
    for seq_nr in out_of_order_payloads.keys().copied() {
        let offset = seq_nr.wrapping_sub(ack_nr);
        if !(2..(2 + (SACK_EXTENSION_BYTES as u16 * 8))).contains(&offset) {
            continue;
        }
        let bit_index = (offset - 2) as usize;
        mask[bit_index / 8] |= 1 << (bit_index % 8);
        any = true;
    }

    if any {
        mask
    } else {
        Vec::new()
    }
}

fn selective_ack_contains(selective_ack: &[u8], ack_nr: u16, seq_nr: u16) -> bool {
    let offset = seq_nr.wrapping_sub(ack_nr);
    if offset < 2 || offset >= 0x8000 {
        return false;
    }

    let bit_index = (offset - 2) as usize;
    selective_ack
        .get(bit_index / 8)
        .is_some_and(|byte| byte & (1 << (bit_index % 8)) != 0)
}

fn seq_lte(lhs: u16, rhs: u16) -> bool {
    lhs == rhs || rhs.wrapping_sub(lhs) < 0x8000
}

fn seq_gt(lhs: u16, rhs: u16) -> bool {
    lhs != rhs && lhs.wrapping_sub(rhs) < 0x8000
}

fn unacked_payload_bytes(unacked_packets: &VecDeque<SentPacket>) -> usize {
    unacked_packets
        .iter()
        .map(|packet| packet.payload.len())
        .sum()
}

fn timestamp_microseconds(start: Instant) -> u32 {
    start.elapsed().as_micros() as u32
}

fn timestamp_difference_microseconds(state: &UtpDriverState) -> u32 {
    state.reply_delay_microseconds
}

fn doubled_duration(duration: Duration) -> Duration {
    duration.checked_mul(2).unwrap_or(Duration::from_secs(60))
}

fn random_connection_id() -> u16 {
    loop {
        let id = rand::random::<u16>();
        if id != u16::MAX {
            return id;
        }
    }
}

fn elapsed_connect_attempts(start: Instant) -> usize {
    let elapsed = start.elapsed().as_millis();
    let retry_ms = CONNECT_RETRY_TIMEOUT.as_millis().max(1);
    (elapsed / retry_ms) as usize
}

fn utp_transport_log_enabled() -> bool {
    std::env::var(UTP_CONNECT_LOG_ENV)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

async fn bind_outbound_socket(remote_addr: SocketAddr) -> io::Result<UdpSocket> {
    let bind_addr = match remote_addr.ip() {
        IpAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        IpAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    };
    UdpSocket::bind(bind_addr).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn packet_round_trips() {
        let packet = UtpPacket {
            packet_type: TYPE_DATA,
            connection_id: 123,
            timestamp_microseconds: 456,
            timestamp_difference_microseconds: 789,
            wnd_size: 1_024,
            seq_nr: 65_530,
            ack_nr: 42,
            selective_ack: Vec::new(),
            payload: b"hello".to_vec(),
        };

        let decoded = UtpPacket::decode(&packet.encode()).unwrap();

        assert_eq!(decoded, packet);
    }

    #[test]
    fn decode_skips_extension_chain() {
        let packet = UtpPacket {
            packet_type: TYPE_DATA,
            connection_id: 123,
            timestamp_microseconds: 456,
            timestamp_difference_microseconds: 789,
            wnd_size: 1_024,
            seq_nr: 65_530,
            ack_nr: 42,
            selective_ack: Vec::new(),
            payload: b"hello".to_vec(),
        };
        let mut bytes = packet.encode();
        bytes[1] = 2;
        bytes.splice(HEADER_LEN..HEADER_LEN, [0, 3, 1, 2, 3]);

        let decoded = UtpPacket::decode(&bytes).unwrap();

        assert_eq!(decoded, packet);
    }

    #[test]
    fn selective_ack_extension_round_trips() {
        let packet = UtpPacket {
            packet_type: TYPE_STATE,
            connection_id: 123,
            timestamp_microseconds: 456,
            timestamp_difference_microseconds: 789,
            wnd_size: 1_024,
            seq_nr: 65_530,
            ack_nr: 42,
            selective_ack: vec![0b0000_0101, 0, 0, 0],
            payload: Vec::new(),
        };

        let bytes = packet.encode();
        assert_eq!(bytes[1], EXT_SELECTIVE_ACK);

        let decoded = UtpPacket::decode(&bytes).unwrap();

        assert_eq!(decoded, packet);
    }

    #[test]
    fn selective_ack_bit_mapping_starts_at_ack_plus_two() {
        let mut out_of_order = BTreeMap::new();
        out_of_order.insert(12, b"later".to_vec());
        out_of_order.insert(14, b"later2".to_vec());

        let mask = selective_ack_for(10, &out_of_order);

        assert_eq!(mask, vec![0b0000_0101, 0, 0, 0]);
        assert!(selective_ack_contains(&mask, 10, 12));
        assert!(!selective_ack_contains(&mask, 10, 13));
        assert!(selective_ack_contains(&mask, 10, 14));
    }

    #[test]
    fn ack_processing_honors_selective_ack() {
        let mut unacked = VecDeque::from([
            SentPacket {
                packet_type: TYPE_DATA,
                seq_nr: 10,
                payload: vec![1],
                sent_at: Instant::now(),
                retransmits: 0,
            },
            SentPacket {
                packet_type: TYPE_DATA,
                seq_nr: 11,
                payload: vec![2],
                sent_at: Instant::now(),
                retransmits: 0,
            },
            SentPacket {
                packet_type: TYPE_DATA,
                seq_nr: 12,
                payload: vec![3],
                sent_at: Instant::now(),
                retransmits: 0,
            },
        ]);

        let outcome = acknowledge_packets(&mut unacked, 10, &[0b0000_0001, 0, 0, 0]);

        assert_eq!(outcome.acked_packets.len(), 2);
        assert_eq!(unacked.len(), 1);
        assert_eq!(unacked.front().unwrap().seq_nr, 11);
    }

    #[test]
    fn congestion_window_reacts_to_delay_and_loss() {
        let mut state = UtpDriverState {
            send_connection_id: 2,
            receive_connection_id: 1,
            next_send_seq_nr: 2,
            last_remote_seq_nr: 76,
            reply_delay_microseconds: 0,
            remote_window_bytes: RECEIVE_WINDOW as usize,
            max_window_bytes: MAX_PACKET_SIZE as f64,
            packet_size: MAX_PACKET_SIZE,
            rtt_microseconds: None,
            rtt_var_microseconds: 0.0,
            retransmit_timeout: INITIAL_RETRANSMIT_TIMEOUT,
            consecutive_timeouts: 0,
            last_ack_nr_seen: 1,
            duplicate_ack_count: 0,
            delay_history: DelayHistory::default(),
            start: Instant::now(),
        };

        state.update_congestion_window(10_000, MAX_PACKET_SIZE);
        let grown = state.max_window_bytes;
        state.update_congestion_window(250_000, MAX_PACKET_SIZE);
        assert!(state.max_window_bytes < grown);
        assert_eq!(state.packet_size, MIN_PACKET_SIZE);

        let before_loss = state.max_window_bytes;
        state.on_packet_loss();
        assert!(state.max_window_bytes <= before_loss);
        assert_eq!(state.packet_size, MIN_PACKET_SIZE);
    }

    #[test]
    fn sequence_comparison_wraps() {
        assert!(seq_lte(65_535, 0));
        assert!(seq_lte(0, 1));
        assert!(!seq_lte(10, 9));
    }

    #[tokio::test]
    async fn outbound_connection_exchanges_payload_with_utp_peer() {
        let server = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut buf = vec![0_u8; 2_048];
            let (n, client_addr) = server.recv_from(&mut buf).await.unwrap();
            let syn = UtpPacket::decode(&buf[..n]).unwrap();
            assert_eq!(syn.packet_type, TYPE_SYN);
            assert_eq!(syn.seq_nr, 1);

            let server_seq_nr = 77;
            let state = UtpPacket {
                packet_type: TYPE_STATE,
                connection_id: syn.connection_id,
                timestamp_microseconds: 1,
                timestamp_difference_microseconds: 0,
                wnd_size: RECEIVE_WINDOW,
                seq_nr: server_seq_nr,
                ack_nr: syn.seq_nr,
                selective_ack: Vec::new(),
                payload: Vec::new(),
            };
            server.send_to(&state.encode(), client_addr).await.unwrap();

            let (n, client_addr) = server.recv_from(&mut buf).await.unwrap();
            let data = UtpPacket::decode(&buf[..n]).unwrap();
            assert_eq!(data.packet_type, TYPE_DATA);
            assert_eq!(data.connection_id, syn.connection_id.wrapping_add(1));
            assert_eq!(data.seq_nr, syn.seq_nr.wrapping_add(1));
            assert_eq!(data.ack_nr, server_seq_nr.wrapping_sub(1));
            assert_eq!(data.payload, b"ping");

            let ack = UtpPacket {
                packet_type: TYPE_STATE,
                connection_id: syn.connection_id,
                timestamp_microseconds: 2,
                timestamp_difference_microseconds: 0,
                wnd_size: RECEIVE_WINDOW,
                seq_nr: server_seq_nr,
                ack_nr: data.seq_nr,
                selective_ack: Vec::new(),
                payload: Vec::new(),
            };
            server.send_to(&ack.encode(), client_addr).await.unwrap();

            let echo = UtpPacket {
                packet_type: TYPE_DATA,
                connection_id: syn.connection_id,
                timestamp_microseconds: 3,
                timestamp_difference_microseconds: 0,
                wnd_size: RECEIVE_WINDOW,
                seq_nr: server_seq_nr,
                ack_nr: data.seq_nr,
                selective_ack: Vec::new(),
                payload: data.payload,
            };
            server.send_to(&echo.encode(), client_addr).await.unwrap();
        });

        let mut connection = UtpPeerTransport::connect(server_addr).await.unwrap();
        connection.stream.write_all(b"ping").await.unwrap();

        let mut echoed = [0_u8; 4];
        connection.stream.read_exact(&mut echoed).await.unwrap();

        assert_eq!(&echoed, b"ping");
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn outbound_connection_selective_acks_and_reorders_payloads() {
        let server = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .unwrap();
        let server_addr = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let mut buf = vec![0_u8; 2_048];
            let (n, client_addr) = server.recv_from(&mut buf).await.unwrap();
            let syn = UtpPacket::decode(&buf[..n]).unwrap();
            assert_eq!(syn.packet_type, TYPE_SYN);

            let server_seq_nr = 77;
            let state = UtpPacket {
                packet_type: TYPE_STATE,
                connection_id: syn.connection_id,
                timestamp_microseconds: 1,
                timestamp_difference_microseconds: 0,
                wnd_size: RECEIVE_WINDOW,
                seq_nr: server_seq_nr,
                ack_nr: syn.seq_nr,
                selective_ack: Vec::new(),
                payload: Vec::new(),
            };
            server.send_to(&state.encode(), client_addr).await.unwrap();

            let second = UtpPacket {
                packet_type: TYPE_DATA,
                connection_id: syn.connection_id,
                timestamp_microseconds: 2,
                timestamp_difference_microseconds: 0,
                wnd_size: RECEIVE_WINDOW,
                seq_nr: server_seq_nr.wrapping_add(1),
                ack_nr: syn.seq_nr,
                selective_ack: Vec::new(),
                payload: b"second".to_vec(),
            };
            server.send_to(&second.encode(), client_addr).await.unwrap();

            let (n, _) = time::timeout(Duration::from_secs(1), server.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
            let sack = UtpPacket::decode(&buf[..n]).unwrap();
            assert_eq!(sack.packet_type, TYPE_STATE);
            assert_eq!(sack.ack_nr, server_seq_nr.wrapping_sub(1));
            assert_eq!(sack.selective_ack, vec![0b0000_0001, 0, 0, 0]);

            let first = UtpPacket {
                packet_type: TYPE_DATA,
                connection_id: syn.connection_id,
                timestamp_microseconds: 3,
                timestamp_difference_microseconds: 0,
                wnd_size: RECEIVE_WINDOW,
                seq_nr: server_seq_nr,
                ack_nr: syn.seq_nr,
                selective_ack: Vec::new(),
                payload: b"first".to_vec(),
            };
            server.send_to(&first.encode(), client_addr).await.unwrap();
        });

        let mut connection = UtpPeerTransport::connect(server_addr).await.unwrap();
        let mut reordered = [0_u8; 11];
        connection.stream.read_exact(&mut reordered).await.unwrap();

        assert_eq!(&reordered, b"firstsecond");
        server_task.await.unwrap();
    }
}
