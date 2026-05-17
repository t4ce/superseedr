// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket as StdUdpSocket};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

const MAX_DATAGRAM_SIZE: usize = 65_535;
const SHARED_UDP_SUBSCRIBER_QUEUE_CAPACITY: usize = 1_024;
const SHARED_UDP_BIND_RETRY_ATTEMPTS: usize = 16;
const SHARED_UDP_BIND_RETRY_DELAY: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SharedUdpFamily {
    Ipv4,
    Ipv6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SharedUdpKey {
    family: SharedUdpFamily,
    bind_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedUdpProtocol {
    Dht,
    Utp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedUdpDatagram {
    pub source: SocketAddr,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
struct SharedUdpInner {
    key: SharedUdpKey,
    socket: Arc<UdpSocket>,
    dht_tx: StdMutex<Option<mpsc::Sender<SharedUdpDatagram>>>,
    utp_tx: StdMutex<Option<mpsc::Sender<SharedUdpDatagram>>>,
    shutdown_tx: watch::Sender<bool>,
    receive_task: StdMutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
pub struct SharedUdpHandle {
    inner: Arc<SharedUdpInner>,
}

static SHARED_UDP_REGISTRY: LazyLock<StdMutex<HashMap<SharedUdpKey, Weak<SharedUdpInner>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

impl SharedUdpKey {
    pub fn new(bind_addr: SocketAddr, family: SharedUdpFamily) -> Self {
        Self {
            family,
            bind_addr: normalize_bind_addr(bind_addr, family),
        }
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}

impl SharedUdpHandle {
    pub async fn bind(bind_addr: SocketAddr, family: SharedUdpFamily) -> io::Result<Self> {
        let requested_key = SharedUdpKey::new(bind_addr, family);
        let socket = {
            let mut attempt = 0usize;
            loop {
                if let Some(handle) = lookup_shared_udp(&requested_key) {
                    return Ok(handle);
                }

                match bind_udp_socket(requested_key.bind_addr, requested_key.family) {
                    Ok(socket) => break Arc::new(socket),
                    Err(error)
                        if error.kind() == io::ErrorKind::AddrInUse
                            && requested_key.bind_addr.port() != 0
                            && attempt < SHARED_UDP_BIND_RETRY_ATTEMPTS =>
                    {
                        attempt += 1;
                        tokio::time::sleep(SHARED_UDP_BIND_RETRY_DELAY).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        let actual_key = SharedUdpKey {
            family: requested_key.family,
            bind_addr: socket.local_addr()?,
        };
        if let Some(handle) = lookup_shared_udp(&actual_key) {
            return Ok(handle);
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(SharedUdpInner {
            key: actual_key,
            socket: socket.clone(),
            dht_tx: StdMutex::new(None),
            utp_tx: StdMutex::new(None),
            shutdown_tx,
            receive_task: StdMutex::new(None),
        });

        let receive_task = spawn_receive_loop(
            socket,
            Arc::downgrade(&inner),
            inner.shutdown_tx.subscribe(),
            shutdown_rx,
        );
        *inner
            .receive_task
            .lock()
            .expect("shared udp receive task lock") = Some(receive_task);

        if requested_key.bind_addr.port() != 0 {
            register_shared_udp(requested_key, &inner);
        }
        register_shared_udp(actual_key, &inner);

        Ok(Self { inner })
    }
}

impl SharedUdpHandle {
    pub fn key(&self) -> SharedUdpKey {
        self.inner.key
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    pub async fn send_to(&self, payload: &[u8], target: SocketAddr) -> io::Result<usize> {
        self.inner.socket.send_to(payload, target).await
    }

    pub fn subscribe(
        &self,
        protocol: SharedUdpProtocol,
    ) -> io::Result<mpsc::Receiver<SharedUdpDatagram>> {
        let (tx, rx) = mpsc::channel(SHARED_UDP_SUBSCRIBER_QUEUE_CAPACITY);
        let slot = match protocol {
            SharedUdpProtocol::Dht => &self.inner.dht_tx,
            SharedUdpProtocol::Utp => &self.inner.utp_tx,
        };
        let mut guard = slot.lock().expect("shared udp subscriber lock");
        if guard.as_ref().is_some_and(|sender| !sender.is_closed()) {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("{protocol:?} UDP subscriber already registered"),
            ));
        }
        *guard = Some(tx);
        Ok(rx)
    }

    pub async fn close_if_unused(&self) {
        if self.has_active_subscribers() {
            return;
        }
        self.shutdown().await;
    }

    pub async fn shutdown(&self) {
        unregister_shared_udp(self.inner.key, &self.inner);
        let _ = self.inner.shutdown_tx.send(true);
        let receive_task = self
            .inner
            .receive_task
            .lock()
            .expect("shared udp receive task lock")
            .take();
        if let Some(receive_task) = receive_task {
            let _ = receive_task.await;
        }
    }

    fn has_active_subscribers(&self) -> bool {
        [SharedUdpProtocol::Dht, SharedUdpProtocol::Utp]
            .into_iter()
            .any(|protocol| {
                let slot = match protocol {
                    SharedUdpProtocol::Dht => &self.inner.dht_tx,
                    SharedUdpProtocol::Utp => &self.inner.utp_tx,
                };
                slot.lock()
                    .expect("shared udp subscriber lock")
                    .as_ref()
                    .is_some_and(|sender| !sender.is_closed())
            })
    }
}

impl Drop for SharedUdpInner {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

fn lookup_shared_udp(key: &SharedUdpKey) -> Option<SharedUdpHandle> {
    let mut registry = SHARED_UDP_REGISTRY
        .lock()
        .expect("shared udp registry lock");
    match registry.get(key).and_then(Weak::upgrade) {
        Some(inner) => Some(SharedUdpHandle { inner }),
        None => {
            registry.remove(key);
            None
        }
    }
}

fn register_shared_udp(key: SharedUdpKey, inner: &Arc<SharedUdpInner>) {
    let mut registry = SHARED_UDP_REGISTRY
        .lock()
        .expect("shared udp registry lock");
    registry.insert(key, Arc::downgrade(inner));
}

fn unregister_shared_udp(key: SharedUdpKey, inner: &Arc<SharedUdpInner>) {
    let mut registry = SHARED_UDP_REGISTRY
        .lock()
        .expect("shared udp registry lock");
    let should_remove = registry
        .get(&key)
        .is_some_and(|registered| Weak::ptr_eq(registered, &Arc::downgrade(inner)));
    if should_remove {
        registry.remove(&key);
    }
}

fn spawn_receive_loop(
    socket: Arc<UdpSocket>,
    inner: Weak<SharedUdpInner>,
    mut inner_shutdown_rx: watch::Receiver<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut buffer = vec![0u8; MAX_DATAGRAM_SIZE];
        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }
                changed = inner_shutdown_rx.changed() => {
                    if changed.is_err() || *inner_shutdown_rx.borrow() {
                        break;
                    }
                }
                result = socket.recv_from(&mut buffer) => {
                    let (len, source) = match result {
                        Ok(result) => result,
                        Err(error) if is_transient_udp_recv_error(&error) => continue,
                        Err(_) => break,
                    };
                    let Some(inner) = inner.upgrade() else {
                        break;
                    };
                    dispatch_datagram(&inner, source, &buffer[..len]);
                }
            }
        }
    })
}

fn dispatch_datagram(inner: &SharedUdpInner, source: SocketAddr, payload: &[u8]) {
    let protocol = if looks_like_utp(payload) {
        SharedUdpProtocol::Utp
    } else if looks_like_dht(payload) {
        SharedUdpProtocol::Dht
    } else {
        return;
    };
    let slot = match protocol {
        SharedUdpProtocol::Dht => &inner.dht_tx,
        SharedUdpProtocol::Utp => &inner.utp_tx,
    };
    let sender = slot
        .lock()
        .expect("shared udp subscriber lock")
        .as_ref()
        .filter(|sender| !sender.is_closed())
        .cloned();
    if let Some(sender) = sender {
        let datagram = SharedUdpDatagram {
            source,
            payload: payload.to_vec(),
        };
        if sender.try_send(datagram).is_err() {
            tracing::debug!(?protocol, %source, "dropping shared UDP datagram for full subscriber queue");
        }
    }
}

fn looks_like_dht(payload: &[u8]) -> bool {
    payload.first().copied() == Some(b'd')
}

fn looks_like_utp(payload: &[u8]) -> bool {
    payload.len() >= 20 && payload[0] & 0x0f == 1 && payload[0] >> 4 <= 4
}

fn normalize_bind_addr(bind_addr: SocketAddr, family: SharedUdpFamily) -> SocketAddr {
    match family {
        SharedUdpFamily::Ipv4 if bind_addr.is_ipv4() => bind_addr,
        SharedUdpFamily::Ipv4 => SocketAddr::from((Ipv4Addr::UNSPECIFIED, bind_addr.port())),
        SharedUdpFamily::Ipv6 if bind_addr.is_ipv6() => bind_addr,
        SharedUdpFamily::Ipv6 => SocketAddr::from((Ipv6Addr::UNSPECIFIED, bind_addr.port())),
    }
}

fn bind_udp_socket(bind_addr: SocketAddr, family: SharedUdpFamily) -> io::Result<UdpSocket> {
    let domain = match family {
        SharedUdpFamily::Ipv4 => Domain::IPV4,
        SharedUdpFamily::Ipv6 => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if matches!(family, SharedUdpFamily::Ipv6) {
        socket.set_only_v6(true)?;
    }
    socket.bind(&SockAddr::from(bind_addr))?;
    socket.set_nonblocking(true)?;
    let std_socket: StdUdpSocket = socket.into();
    UdpSocket::from_std(std_socket)
}

fn is_transient_udp_recv_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::Interrupted
            | io::ErrorKind::TimedOut
    )
}

pub fn family_for_addr(addr: SocketAddr) -> SharedUdpFamily {
    match addr.ip() {
        IpAddr::V4(_) => SharedUdpFamily::Ipv4,
        IpAddr::V6(_) => SharedUdpFamily::Ipv6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shared_udp_routes_dht_and_utp_on_one_socket() {
        let handle = SharedUdpHandle::bind(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            SharedUdpFamily::Ipv4,
        )
        .await
        .unwrap();
        let mut dht_rx = handle.subscribe(SharedUdpProtocol::Dht).unwrap();
        let mut utp_rx = handle.subscribe(SharedUdpProtocol::Utp).unwrap();
        let sender = UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .unwrap();
        let local_addr = handle.local_addr().unwrap();

        sender.send_to(b"d1:eli201ee", local_addr).await.unwrap();
        sender
            .send_to(
                &[
                    (4 << 4) | 1,
                    0,
                    0,
                    1,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    0,
                    0,
                ],
                local_addr,
            )
            .await
            .unwrap();

        let dht = dht_rx.recv().await.unwrap();
        let utp = utp_rx.recv().await.unwrap();

        assert_eq!(dht.payload, b"d1:eli201ee");
        assert_eq!(utp.payload[0] >> 4, 4);
        handle.shutdown().await;
    }
}
