// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, LazyLock, Mutex as StdMutex, Weak},
    time::Duration,
};

use tokio::{
    io::{self as tokio_io, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream},
    sync::{mpsc, watch},
    time::{self, Instant},
};

use crate::networking::shared_udp::{
    family_for_addr, SharedUdpDatagram, SharedUdpFamily, SharedUdpHandle, SharedUdpKey,
    SharedUdpProtocol,
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
const MAX_PACKET_SIZE: usize = 2_560;
const STREAM_BUFFER: usize = 256 * 1024;
const MAX_INFLIGHT_PACKETS: usize = 64;
const CONNECT_RETRIES: usize = 4;
const CONNECT_RETRY_TIMEOUT: Duration = Duration::from_millis(400);
const ENDPOINT_BIND_RETRY_ATTEMPTS: usize = 16;
const ENDPOINT_BIND_RETRY_DELAY: Duration = Duration::from_millis(1);
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

/// Homegrown BEP 29/uTP transport.
///
/// This supports stream-like connections over the shared UDP runtime with packet
/// reliability, selective ACK handling, adaptive retransmit timeouts, and
/// LEDBAT-style delay-based congestion control.
pub struct UtpPeerTransport;

impl UtpPeerTransport {
    #[allow(dead_code)]
    pub async fn connect(remote_addr: SocketAddr) -> io::Result<PeerConnection> {
        Self::connect_from_port(remote_addr, 0).await
    }

    pub async fn connect_from_port(
        remote_addr: SocketAddr,
        local_port: u16,
    ) -> io::Result<PeerConnection> {
        let endpoint =
            UtpEndpoint::bind(SocketAddr::new(bind_ip_for(remote_addr), local_port)).await?;

        let start = Instant::now();
        let receive_connection_id = random_connection_id();
        let send_connection_id = receive_connection_id.wrapping_add(1);
        let initial_seq_nr = 1;
        let (mut incoming_packets, session_guard) =
            endpoint.register_session(remote_addr, receive_connection_id)?;

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

        let state = loop {
            endpoint.send_bytes(remote_addr, &syn_bytes).await?;

            match time::timeout(CONNECT_RETRY_TIMEOUT, incoming_packets.recv()).await {
                Ok(Some(packet)) => {
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
                Ok(None) => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "uTP shared UDP session closed during connect",
                    ));
                }
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
        let io = UtpSessionIo {
            endpoint,
            remote_addr,
            incoming_packets,
            _session_guard: session_guard,
        };
        tokio::spawn(async move {
            if let Err(error) = run_utp_driver(io, driver_stream, state).await {
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

    pub async fn bind_listener(port: u16) -> io::Result<UtpListenerSet> {
        UtpListenerSet::bind(port).await
    }
}

#[derive(Clone)]
pub struct UtpListenerSet {
    ipv4: Option<UtpListener>,
    ipv6: Option<UtpListener>,
}

impl UtpListenerSet {
    async fn bind(port: u16) -> io::Result<Self> {
        let ipv4 = match UtpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
            .await
        {
            Ok(listener) => Some(listener),
            Err(error) => {
                tracing::warn!(%error, "IPv4 uTP listener bind failed");
                None
            }
        };
        let ipv6_port = match (port, ipv4.as_ref().and_then(UtpListener::local_port)) {
            (0, Some(bound_port)) => bound_port,
            _ => port,
        };

        let ipv6 = match UtpListener::bind(SocketAddr::new(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            ipv6_port,
        ))
        .await
        {
            Ok(listener) => Some(listener),
            Err(error) => {
                tracing::warn!(%error, "IPv6 uTP listener bind failed");
                None
            }
        };

        if ipv4.is_none() && ipv6.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "failed to bind IPv4 or IPv6 uTP listener",
            ));
        }

        Ok(Self { ipv4, ipv6 })
    }

    pub async fn accept(&self) -> io::Result<PeerConnection> {
        match (&self.ipv4, &self.ipv6) {
            (Some(ipv4), Some(ipv6)) => {
                tokio::select! {
                    res = ipv4.accept() => res,
                    res = ipv6.accept() => res,
                }
            }
            (Some(ipv4), None) => ipv4.accept().await,
            (None, Some(ipv6)) => ipv6.accept().await,
            (None, None) => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "no uTP listener is currently bound",
            )),
        }
    }

    pub fn local_port(&self) -> Option<u16> {
        self.ipv4
            .as_ref()
            .or(self.ipv6.as_ref())
            .and_then(UtpListener::local_port)
    }
}

#[derive(Clone)]
pub struct UtpListener {
    endpoint: UtpEndpoint,
    accept_rx: Arc<tokio::sync::Mutex<mpsc::Receiver<PeerConnection>>>,
}

impl UtpListener {
    async fn bind(bind_addr: SocketAddr) -> io::Result<Self> {
        let endpoint = UtpEndpoint::bind(bind_addr).await?;
        let (accept_tx, accept_rx) = mpsc::channel(128);
        endpoint.set_accept_sender(accept_tx)?;
        Ok(Self {
            endpoint,
            accept_rx: Arc::new(tokio::sync::Mutex::new(accept_rx)),
        })
    }

    async fn accept(&self) -> io::Result<PeerConnection> {
        self.accept_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::ConnectionAborted, "uTP listener closed"))
    }

    fn local_port(&self) -> Option<u16> {
        self.endpoint.local_addr().ok().map(|addr| addr.port())
    }
}

#[derive(Clone)]
struct UtpEndpoint {
    inner: Arc<UtpEndpointInner>,
}

struct UtpEndpointInner {
    udp: SharedUdpHandle,
    sessions: StdMutex<HashMap<UtpSessionKey, mpsc::UnboundedSender<UtpPacket>>>,
    inbound_syn_responses: StdMutex<HashMap<UtpSessionKey, UtpPacket>>,
    accept_tx: StdMutex<Option<mpsc::Sender<PeerConnection>>>,
    shutdown_tx: watch::Sender<bool>,
    task: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

struct UtpSessionIo {
    endpoint: UtpEndpoint,
    remote_addr: SocketAddr,
    incoming_packets: mpsc::UnboundedReceiver<UtpPacket>,
    _session_guard: UtpSessionGuard,
}

struct UtpSessionGuard {
    endpoint: Weak<UtpEndpointInner>,
    key: UtpSessionKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct UtpSessionKey {
    remote_addr: SocketAddr,
    connection_id: u16,
}

static UTP_ENDPOINT_REGISTRY: LazyLock<StdMutex<HashMap<SharedUdpKey, Weak<UtpEndpointInner>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

impl UtpEndpoint {
    async fn bind(bind_addr: SocketAddr) -> io::Result<Self> {
        let family = family_for_addr(bind_addr);
        let requested_key = SharedUdpKey::new(bind_addr, family);
        let mut attempt = 0usize;

        loop {
            if let Some(endpoint) = lookup_utp_endpoint(&requested_key) {
                return Ok(endpoint);
            }

            let udp = SharedUdpHandle::bind(bind_addr, family).await?;
            let actual_key = udp.key();
            if let Some(endpoint) = lookup_utp_endpoint(&actual_key) {
                return Ok(endpoint);
            }

            let datagram_rx = match udp.subscribe(SharedUdpProtocol::Utp) {
                Ok(datagram_rx) => datagram_rx,
                Err(error)
                    if error.kind() == io::ErrorKind::AddrInUse
                        && requested_key.bind_addr().port() != 0
                        && attempt < ENDPOINT_BIND_RETRY_ATTEMPTS =>
                {
                    attempt += 1;
                    time::sleep(ENDPOINT_BIND_RETRY_DELAY).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let inner = Arc::new(UtpEndpointInner {
                udp,
                sessions: StdMutex::new(HashMap::new()),
                inbound_syn_responses: StdMutex::new(HashMap::new()),
                accept_tx: StdMutex::new(None),
                shutdown_tx,
                task: StdMutex::new(None),
            });
            let task = spawn_utp_demux_task(Arc::downgrade(&inner), datagram_rx, shutdown_rx);
            *inner.task.lock().expect("uTP endpoint task lock") = Some(task);

            if requested_key.bind_addr().port() != 0 {
                register_utp_endpoint(requested_key, &inner);
            }
            register_utp_endpoint(actual_key, &inner);

            return Ok(Self { inner });
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.udp.local_addr()
    }

    fn set_accept_sender(&self, accept_tx: mpsc::Sender<PeerConnection>) -> io::Result<()> {
        let mut guard = self.inner.accept_tx.lock().expect("uTP accept sender lock");
        if guard.as_ref().is_some_and(|sender| !sender.is_closed()) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "uTP listener already registered for endpoint",
            ));
        }
        *guard = Some(accept_tx);
        Ok(())
    }

    fn register_session(
        &self,
        remote_addr: SocketAddr,
        connection_id: u16,
    ) -> io::Result<(mpsc::UnboundedReceiver<UtpPacket>, UtpSessionGuard)> {
        let key = UtpSessionKey {
            remote_addr,
            connection_id,
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let mut sessions = self.inner.sessions.lock().expect("uTP session map lock");
        if sessions.get(&key).is_some_and(|sender| !sender.is_closed()) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "uTP session already registered",
            ));
        }
        sessions.insert(key, tx);
        Ok((
            rx,
            UtpSessionGuard {
                endpoint: Arc::downgrade(&self.inner),
                key,
            },
        ))
    }

    async fn send_packet(&self, remote_addr: SocketAddr, packet: &UtpPacket) -> io::Result<usize> {
        self.send_bytes(remote_addr, &packet.encode()).await
    }

    async fn send_bytes(&self, remote_addr: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.inner.udp.send_to(bytes, remote_addr).await
    }
}

impl Drop for UtpEndpointInner {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

impl Drop for UtpSessionGuard {
    fn drop(&mut self) {
        let Some(endpoint) = self.endpoint.upgrade() else {
            return;
        };
        if let Ok(mut sessions) = endpoint.sessions.lock() {
            sessions.remove(&self.key);
        };
        if let Ok(mut inbound_syn_responses) = endpoint.inbound_syn_responses.lock() {
            inbound_syn_responses.remove(&self.key);
        };
    }
}

impl UtpSessionIo {
    async fn send(&self, packet: &UtpPacket) -> io::Result<usize> {
        self.endpoint.send_packet(self.remote_addr, packet).await
    }

    async fn recv(&mut self) -> io::Result<UtpPacket> {
        self.incoming_packets.recv().await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "uTP shared UDP session closed",
            )
        })
    }
}

fn lookup_utp_endpoint(key: &SharedUdpKey) -> Option<UtpEndpoint> {
    let mut registry = UTP_ENDPOINT_REGISTRY
        .lock()
        .expect("uTP endpoint registry lock");
    match registry.get(key).and_then(Weak::upgrade) {
        Some(inner) => Some(UtpEndpoint { inner }),
        None => {
            registry.remove(key);
            None
        }
    }
}

fn register_utp_endpoint(key: SharedUdpKey, inner: &Arc<UtpEndpointInner>) {
    let mut registry = UTP_ENDPOINT_REGISTRY
        .lock()
        .expect("uTP endpoint registry lock");
    registry.insert(key, Arc::downgrade(inner));
}

fn spawn_utp_demux_task(
    endpoint: Weak<UtpEndpointInner>,
    mut datagram_rx: mpsc::UnboundedReceiver<SharedUdpDatagram>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                datagram = datagram_rx.recv() => {
                    let Some(datagram) = datagram else {
                        break;
                    };
                    let Some(endpoint) = endpoint.upgrade() else {
                        break;
                    };
                    dispatch_utp_datagram(&endpoint, datagram).await;
                }
            }
        }
    })
}

async fn dispatch_utp_datagram(endpoint: &Arc<UtpEndpointInner>, datagram: SharedUdpDatagram) {
    let Ok(packet) = UtpPacket::decode(&datagram.payload) else {
        return;
    };
    if packet.packet_type == TYPE_SYN {
        handle_inbound_syn(endpoint, datagram.source, packet).await;
        return;
    }

    let key = UtpSessionKey {
        remote_addr: datagram.source,
        connection_id: packet.connection_id,
    };
    let sender = endpoint
        .sessions
        .lock()
        .expect("uTP session map lock")
        .get(&key)
        .cloned();
    if let Some(sender) = sender {
        let _ = sender.send(packet);
    }
}

async fn handle_inbound_syn(
    endpoint: &Arc<UtpEndpointInner>,
    remote_addr: SocketAddr,
    syn: UtpPacket,
) {
    let accept_tx = endpoint
        .accept_tx
        .lock()
        .expect("uTP accept sender lock")
        .as_ref()
        .filter(|sender| !sender.is_closed())
        .cloned();
    let Some(accept_tx) = accept_tx else {
        return;
    };

    let endpoint = UtpEndpoint {
        inner: endpoint.clone(),
    };
    let receive_connection_id = syn.connection_id.wrapping_add(1);
    let session_key = UtpSessionKey {
        remote_addr,
        connection_id: receive_connection_id,
    };
    let (incoming_packets, session_guard) =
        match endpoint.register_session(remote_addr, receive_connection_id) {
            Ok(registration) => registration,
            Err(error) if error.kind() == io::ErrorKind::AddrInUse => {
                let response = endpoint
                    .inner
                    .inbound_syn_responses
                    .lock()
                    .expect("uTP inbound SYN response map lock")
                    .get(&session_key)
                    .cloned();
                if let Some(response) = response {
                    let _ = endpoint.send_packet(remote_addr, &response).await;
                }
                return;
            }
            Err(_) => return,
        };

    let start = Instant::now();
    let server_seq_nr = random_connection_id();
    let mut state = UtpDriverState {
        send_connection_id: syn.connection_id,
        receive_connection_id,
        next_send_seq_nr: server_seq_nr,
        last_remote_seq_nr: syn.seq_nr,
        reply_delay_microseconds: timestamp_microseconds(start)
            .wrapping_sub(syn.timestamp_microseconds),
        remote_window_bytes: syn.wnd_size as usize,
        max_window_bytes: MAX_PACKET_SIZE as f64,
        packet_size: MAX_PACKET_SIZE,
        rtt_microseconds: None,
        rtt_var_microseconds: 0.0,
        retransmit_timeout: INITIAL_RETRANSMIT_TIMEOUT,
        consecutive_timeouts: 0,
        last_ack_nr_seen: syn.ack_nr,
        duplicate_ack_count: 0,
        delay_history: DelayHistory::default(),
        start,
    };
    state.record_received_packet(&syn);

    let (app_stream, driver_stream) = tokio_io::duplex(STREAM_BUFFER);
    let io = UtpSessionIo {
        endpoint,
        remote_addr,
        incoming_packets,
        _session_guard: session_guard,
    };
    let state_packet = UtpPacket {
        packet_type: TYPE_STATE,
        connection_id: state.send_connection_id,
        timestamp_microseconds: timestamp_microseconds(state.start),
        timestamp_difference_microseconds: timestamp_difference_microseconds(&state),
        wnd_size: RECEIVE_WINDOW,
        seq_nr: server_seq_nr,
        ack_nr: state.last_remote_seq_nr,
        selective_ack: Vec::new(),
        payload: Vec::new(),
    };
    io.endpoint
        .inner
        .inbound_syn_responses
        .lock()
        .expect("uTP inbound SYN response map lock")
        .insert(session_key, state_packet.clone());
    if io.send(&state_packet).await.is_err() {
        return;
    }

    tokio::spawn(async move {
        if let Err(error) = run_utp_driver(io, driver_stream, state).await {
            if utp_transport_log_enabled() {
                tracing::info!(%remote_addr, %error, "inbound uTP driver stopped");
            }
            tracing::debug!(%remote_addr, %error, "inbound uTP driver stopped");
        }
    });

    let connection = PeerConnection::new(
        app_stream,
        PeerEndpoint::utp(remote_addr),
        remote_addr,
        PeerConnectionDirection::Incoming,
    );
    let _ = accept_tx.send(connection).await;
}

fn bind_ip_for(remote_addr: SocketAddr) -> IpAddr {
    match family_for_addr(remote_addr) {
        SharedUdpFamily::Ipv4 => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        SharedUdpFamily::Ipv6 => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
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
            if extension_len < SACK_EXTENSION_BYTES
                || !extension_len.is_multiple_of(SACK_EXTENSION_BYTES)
            {
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
    fn ack_nr(&self) -> u16 {
        self.last_remote_seq_nr
    }

    fn accepts_remote_payload_sequence(&self, seq_nr: u16) -> bool {
        seq_nr == self.last_remote_seq_nr.wrapping_add(1)
    }

    fn record_remote_payload_sequence(&mut self, seq_nr: u16) {
        self.last_remote_seq_nr = seq_nr;
    }

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
        io: &UtpSessionIo,
        unacked_packets: &mut VecDeque<SentPacket>,
        outcome: AckOutcome,
        packet: &UtpPacket,
        count_duplicate_acks: bool,
    ) -> io::Result<()> {
        if outcome.advanced_ack {
            self.duplicate_ack_count = 0;
            self.last_ack_nr_seen = packet.ack_nr;
        } else if count_duplicate_acks && packet.ack_nr == self.last_ack_nr_seen {
            self.duplicate_ack_count = self.duplicate_ack_count.saturating_add(1);
            if self.duplicate_ack_count >= 3 {
                if retransmit_packet(io, self, unacked_packets, packet.ack_nr.wrapping_add(1))
                    .await?
                {
                    self.on_packet_loss();
                }
                self.duplicate_ack_count = 0;
            }
        } else {
            self.duplicate_ack_count = 0;
            self.last_ack_nr_seen = packet.ack_nr;
        }

        if !outcome.fast_retransmit.is_empty() {
            let mut retransmitted = false;
            for seq_nr in outcome.fast_retransmit {
                retransmitted |= retransmit_packet(io, self, unacked_packets, seq_nr).await?;
            }
            if retransmitted {
                self.on_packet_loss();
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
    mut io: UtpSessionIo,
    local_stream: DuplexStream,
    mut state: UtpDriverState,
) -> io::Result<()> {
    let (mut local_reader, mut local_writer) = tokio_io::split(local_stream);
    let mut local_buf = vec![0_u8; MAX_PACKET_SIZE];
    let mut pending_payloads: VecDeque<Vec<u8>> = VecDeque::new();
    let mut unacked_packets: VecDeque<SentPacket> = VecDeque::new();
    let mut out_of_order_payloads: BTreeMap<u16, Vec<u8>> = BTreeMap::new();
    let mut local_eof = false;
    let mut retransmit_tick = time::interval(RETRANSMIT_TICK);

    loop {
        flush_pending_payloads(&io, &mut state, &mut pending_payloads, &mut unacked_packets)
            .await?;

        if local_eof && pending_payloads.is_empty() && unacked_packets.is_empty() {
            send_control_packet(&io, &mut state, TYPE_FIN).await?;
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
            recv_result = io.recv() => {
                let packet = recv_result?;
                process_incoming_packet(
                    &io,
                    &mut local_writer,
                    &mut state,
                    &mut unacked_packets,
                    &mut out_of_order_payloads,
                    packet,
                ).await?;
            }
            _ = retransmit_tick.tick() => {
                retransmit_due_packets(&io, &mut state, &mut unacked_packets).await?;
            }
        }
    }
}

async fn flush_pending_payloads(
    io: &UtpSessionIo,
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
        io.send(&packet).await?;
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
    io: &UtpSessionIo,
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
        TYPE_STATE | TYPE_DATA | TYPE_FIN => {
            let outcome =
                acknowledge_packets(unacked_packets, packet.ack_nr, &packet.selective_ack);
            let count_duplicate_acks = packet.packet_type == TYPE_STATE;
            state
                .apply_ack_outcome(io, unacked_packets, outcome, &packet, count_duplicate_acks)
                .await?;
        }
        _ => {}
    }

    match packet.packet_type {
        TYPE_STATE => {}
        TYPE_DATA => {
            let expected_seq_nr = state.last_remote_seq_nr.wrapping_add(1);
            if state.accepts_remote_payload_sequence(packet.seq_nr) {
                if !packet.payload.is_empty() {
                    local_writer.write_all(&packet.payload).await?;
                }
                state.record_remote_payload_sequence(packet.seq_nr);
                deliver_buffered_payloads(local_writer, state, out_of_order_payloads).await?;
            } else if seq_gt(packet.seq_nr, expected_seq_nr)
                && out_of_order_payloads.len() < MAX_OUT_OF_ORDER_PACKETS
            {
                out_of_order_payloads
                    .entry(packet.seq_nr)
                    .or_insert(packet.payload);
            }
            send_state_packet(io, state, out_of_order_payloads).await?;
        }
        TYPE_FIN => {
            if state.accepts_remote_payload_sequence(packet.seq_nr) {
                state.record_remote_payload_sequence(packet.seq_nr);
            }
            send_state_packet(io, state, out_of_order_payloads).await?;
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
    io: &UtpSessionIo,
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
            ack_nr: state.ack_nr(),
            selective_ack: Vec::new(),
            payload: sent.payload.clone(),
        };
        io.send(&packet).await?;
    }

    if saw_timeout {
        state.on_packet_loss();
        state.consecutive_timeouts = state.consecutive_timeouts.saturating_add(1);
        state.retransmit_timeout = doubled_duration(state.retransmit_timeout);
    }

    Ok(())
}

async fn send_control_packet(
    io: &UtpSessionIo,
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
        ack_nr: state.ack_nr(),
        selective_ack: Vec::new(),
        payload: Vec::new(),
    };
    io.send(&packet).await?;
    Ok(())
}

async fn send_state_packet(
    io: &UtpSessionIo,
    state: &UtpDriverState,
    out_of_order_payloads: &BTreeMap<u16, Vec<u8>>,
) -> io::Result<()> {
    let ack_nr = state.ack_nr();
    let packet = UtpPacket {
        packet_type: TYPE_STATE,
        connection_id: state.send_connection_id,
        timestamp_microseconds: timestamp_microseconds(state.start),
        timestamp_difference_microseconds: timestamp_difference_microseconds(state),
        wnd_size: advertised_window(out_of_order_payloads),
        seq_nr: state.next_send_seq_nr,
        ack_nr,
        selective_ack: selective_ack_for(ack_nr, out_of_order_payloads),
        payload: Vec::new(),
    };
    io.send(&packet).await?;
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
        ack_nr: state.ack_nr(),
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
    io: &UtpSessionIo,
    state: &UtpDriverState,
    unacked_packets: &mut VecDeque<SentPacket>,
    seq_nr: u16,
) -> io::Result<bool> {
    let Some(sent) = unacked_packets
        .iter_mut()
        .find(|packet| packet.seq_nr == seq_nr)
    else {
        return Ok(false);
    };

    if sent.retransmits > 0 && sent.sent_at.elapsed() < state.retransmit_timeout {
        return Ok(false);
    }

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
        ack_nr: state.ack_nr(),
        selective_ack: Vec::new(),
        payload: sent.payload.clone(),
    };
    io.send(&packet).await?;
    Ok(true)
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
    if !(2..0x8000).contains(&offset) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UdpSocket;

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

    #[tokio::test]
    async fn inbound_listener_resends_state_for_retransmitted_syn() {
        let listener = UtpPeerTransport::bind_listener(0).await.unwrap();
        let listen_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            listener.local_port().unwrap(),
        );
        let client = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .unwrap();
        let syn = UtpPacket {
            packet_type: TYPE_SYN,
            connection_id: 0x1234,
            timestamp_microseconds: 10,
            timestamp_difference_microseconds: 0,
            wnd_size: RECEIVE_WINDOW,
            seq_nr: 9,
            ack_nr: 0,
            selective_ack: Vec::new(),
            payload: Vec::new(),
        };
        let mut buf = vec![0_u8; 2_048];

        client.send_to(&syn.encode(), listen_addr).await.unwrap();
        let (n, _) = time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let first_state = UtpPacket::decode(&buf[..n]).unwrap();
        assert_eq!(first_state.packet_type, TYPE_STATE);
        assert_eq!(first_state.connection_id, syn.connection_id);
        assert_eq!(first_state.ack_nr, syn.seq_nr);

        client.send_to(&syn.encode(), listen_addr).await.unwrap();
        let (n, _) = time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let retransmitted_state = UtpPacket::decode(&buf[..n]).unwrap();
        assert_eq!(retransmitted_state, first_state);

        let _accepted = time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        assert!(
            time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err(),
            "duplicate SYN should not enqueue a second accepted connection"
        );
    }

    #[tokio::test]
    async fn inbound_listener_accepts_utp_stream() {
        let listener = UtpPeerTransport::bind_listener(0).await.unwrap();
        let listen_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            listener.local_port().unwrap(),
        );

        let accept_task = tokio::spawn(async move {
            let mut connection = listener.accept().await.unwrap();
            assert_eq!(
                connection.endpoint,
                PeerEndpoint::utp(connection.remote_addr)
            );
            assert_eq!(connection.direction, PeerConnectionDirection::Incoming);

            let mut payload = [0_u8; 4];
            connection.stream.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"ping");

            connection.stream.write_all(b"pong").await.unwrap();
            time::sleep(Duration::from_millis(100)).await;
        });

        let mut client = UtpPeerTransport::connect(listen_addr).await.unwrap();
        client.stream.write_all(b"ping").await.unwrap();

        let mut echoed = [0_u8; 4];
        client.stream.read_exact(&mut echoed).await.unwrap();
        assert_eq!(&echoed, b"pong");

        accept_task.await.unwrap();
    }
}
