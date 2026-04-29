// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::krpc::{KrpcErrorEnvelope, KrpcIncomingQuery, KrpcResponseBody, KrpcResponseEnvelope};
use super::peer_store::PeerStore;
use super::routing::RoutingTable;
use super::token::TokenService;
use super::types::{AddressFamily, CompactNode, CompactPeer, InfoHash, NodeId, NodeRecord};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant, SystemTime};

const ERROR_PROTOCOL: i64 = 203;
const RATE_LIMITER_IDLE_TTL: Duration = Duration::from_secs(300);
const RATE_LIMITER_PRUNE_INTERVAL: Duration = Duration::from_secs(30);
const MAX_RATE_LIMITER_ENTRIES: usize = 16_384;
const DEFAULT_RESPONSE_BYTES_PER_SECOND: usize = 32 * 1024;
const DEFAULT_RESPONSE_BURST_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct InboundConfig {
    pub family: AddressFamily,
    pub max_queries_per_second: usize,
    pub burst_capacity: usize,
    pub response_bytes_per_second: usize,
    pub response_burst_bytes: usize,
    pub closest_nodes_limit: usize,
}

impl Default for InboundConfig {
    fn default() -> Self {
        Self {
            family: AddressFamily::Ipv4,
            max_queries_per_second: 64,
            burst_capacity: 128,
            response_bytes_per_second: DEFAULT_RESPONSE_BYTES_PER_SECOND,
            response_burst_bytes: DEFAULT_RESPONSE_BURST_BYTES,
            closest_nodes_limit: 8,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRequestContext {
    pub source: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundAction {
    Respond(KrpcResponseEnvelope),
    Error(KrpcErrorEnvelope),
    Drop,
}

#[derive(Debug, Clone)]
struct RateLimiter {
    last_refill_at: Instant,
    last_seen_at: Instant,
    tokens: f64,
    response_last_refill_at: Instant,
    response_tokens: f64,
}

#[derive(Debug, Clone)]
pub struct InboundActor {
    config: InboundConfig,
    per_ip_rate_limits: HashMap<IpAddr, RateLimiter>,
    last_rate_limiter_prune_at: Option<Instant>,
}

impl InboundActor {
    pub fn new(config: InboundConfig) -> Self {
        Self {
            config,
            per_ip_rate_limits: HashMap::new(),
            last_rate_limiter_prune_at: None,
        }
    }

    pub fn family(&self) -> AddressFamily {
        self.config.family
    }

    pub fn config(&self) -> &InboundConfig {
        &self.config
    }

    #[allow(clippy::too_many_arguments)]
    pub fn handle_query(
        &mut self,
        ctx: InboundRequestContext,
        query: KrpcIncomingQuery,
        local_node_id: NodeId,
        routing: &mut RoutingTable,
        cross_family_routing: Option<&RoutingTable>,
        token_service: &mut TokenService,
        peer_store: &mut PeerStore,
        now: Instant,
        wall_clock: SystemTime,
    ) -> InboundAction {
        if ctx.source.is_ipv4() != matches!(self.config.family, AddressFamily::Ipv4) {
            return InboundAction::Drop;
        }

        if !self.allow_query(ctx.source.ip(), now) {
            return InboundAction::Drop;
        }

        let transaction_id = query.transaction_id().to_vec();
        let requester_id = match query.requester_id() {
            Some(node_id) => node_id,
            None => {
                return self.error_to(
                    ctx.source.ip(),
                    KrpcErrorEnvelope::new(&transaction_id, ERROR_PROTOCOL, "invalid node id"),
                    now,
                );
            }
        };

        remember_inbound_node(routing, ctx.source, requester_id, now);

        match query {
            KrpcIncomingQuery::Ping { .. } => self.respond_to(
                ctx.source,
                KrpcResponseEnvelope::new(&transaction_id, KrpcResponseBody::pong(local_node_id)),
                now,
            ),
            KrpcIncomingQuery::FindNode { args, .. } => {
                let Ok(target) = NodeId::try_from(args.target.as_ref()) else {
                    return self.error_to(
                        ctx.source.ip(),
                        KrpcErrorEnvelope::new(&transaction_id, ERROR_PROTOCOL, "invalid target"),
                        now,
                    );
                };

                let nodes = self.closest_nodes_for(routing, target, ctx.source, now);
                let mut body =
                    KrpcResponseBody::with_nodes(local_node_id, &nodes, self.config.family);
                self.append_requested_cross_family_nodes(
                    &mut body,
                    cross_family_routing,
                    |family| args.wants_family(family),
                    target,
                    ctx.source,
                    now,
                );
                self.respond_to(
                    ctx.source,
                    KrpcResponseEnvelope::new(&transaction_id, body),
                    now,
                )
            }
            KrpcIncomingQuery::GetPeers { args, .. } => {
                let Ok(info_hash) = InfoHash::try_from(args.info_hash.as_ref()) else {
                    return self.error_to(
                        ctx.source.ip(),
                        KrpcErrorEnvelope::new(
                            &transaction_id,
                            ERROR_PROTOCOL,
                            "invalid info_hash",
                        ),
                        now,
                    );
                };

                let token = token_service.mint_for(ctx.source.ip(), info_hash, now);
                let peers = peer_store.peers_for(info_hash, self.config.family, wall_clock);
                let nodes = self.closest_nodes_for(routing, info_hash.into(), ctx.source, now);
                let mut body = if peers.is_empty() {
                    KrpcResponseBody::with_closest_nodes(
                        local_node_id,
                        &nodes,
                        self.config.family,
                        &token,
                    )
                } else {
                    KrpcResponseBody::with_peers_and_nodes(
                        local_node_id,
                        &peers,
                        &nodes,
                        self.config.family,
                        &token,
                    )
                };
                self.append_requested_cross_family_nodes(
                    &mut body,
                    cross_family_routing,
                    |family| args.wants_family(family),
                    info_hash.into(),
                    ctx.source,
                    now,
                );

                self.respond_to(
                    ctx.source,
                    KrpcResponseEnvelope::new(&transaction_id, body),
                    now,
                )
            }
            KrpcIncomingQuery::AnnouncePeer { args, .. } => {
                let Ok(info_hash) = InfoHash::try_from(args.info_hash.as_ref()) else {
                    return self.error_to(
                        ctx.source.ip(),
                        KrpcErrorEnvelope::new(
                            &transaction_id,
                            ERROR_PROTOCOL,
                            "invalid info_hash",
                        ),
                        now,
                    );
                };

                if !token_service.validate_for(ctx.source.ip(), info_hash, args.token.as_ref(), now)
                {
                    return self.error_to(
                        ctx.source.ip(),
                        KrpcErrorEnvelope::new(&transaction_id, ERROR_PROTOCOL, "invalid token"),
                        now,
                    );
                }

                let port = if args.implied_port.unwrap_or_default() != 0 {
                    ctx.source.port()
                } else {
                    args.port
                };

                if port == 0 {
                    return self.error_to(
                        ctx.source.ip(),
                        KrpcErrorEnvelope::new(&transaction_id, ERROR_PROTOCOL, "invalid port"),
                        now,
                    );
                }

                let peer = CompactPeer {
                    addr: SocketAddr::new(ctx.source.ip(), port),
                };
                peer_store.insert(info_hash, peer, wall_clock);

                self.respond_to(
                    ctx.source,
                    KrpcResponseEnvelope::new(
                        &transaction_id,
                        KrpcResponseBody::pong(local_node_id),
                    ),
                    now,
                )
            }
        }
    }

    fn allow_query(&mut self, source_ip: IpAddr, now: Instant) -> bool {
        self.prune_stale_rate_limiters(now);
        if !self.per_ip_rate_limits.contains_key(&source_ip)
            && self.per_ip_rate_limits.len() >= MAX_RATE_LIMITER_ENTRIES
        {
            return false;
        }

        let burst = self
            .config
            .burst_capacity
            .max(self.config.max_queries_per_second.max(1));
        let fill_rate = self.config.max_queries_per_second.max(1) as f64;
        let response_burst = self.config.response_burst_bytes.max(1);
        let limiter = self
            .per_ip_rate_limits
            .entry(source_ip)
            .or_insert_with(|| RateLimiter {
                last_refill_at: now,
                last_seen_at: now,
                tokens: burst as f64,
                response_last_refill_at: now,
                response_tokens: response_burst as f64,
            });

        let elapsed = now.saturating_duration_since(limiter.last_refill_at);
        limiter.last_refill_at = now;
        limiter.last_seen_at = now;
        limiter.tokens = (limiter.tokens + elapsed.as_secs_f64() * fill_rate).min(burst as f64);
        if limiter.tokens < 1.0 {
            return false;
        }

        limiter.tokens -= 1.0;
        true
    }

    fn allow_response_bytes(&mut self, source_ip: IpAddr, bytes: usize, now: Instant) -> bool {
        self.prune_stale_rate_limiters(now);
        if !self.per_ip_rate_limits.contains_key(&source_ip)
            && self.per_ip_rate_limits.len() >= MAX_RATE_LIMITER_ENTRIES
        {
            return false;
        }

        let burst = self.config.response_burst_bytes.max(1);
        let query_burst = self.config.burst_capacity.max(1);
        let fill_rate = self.config.response_bytes_per_second.max(1) as f64;
        let limiter = self
            .per_ip_rate_limits
            .entry(source_ip)
            .or_insert_with(|| RateLimiter {
                last_refill_at: now,
                last_seen_at: now,
                tokens: query_burst as f64,
                response_last_refill_at: now,
                response_tokens: burst as f64,
            });

        let elapsed = now.saturating_duration_since(limiter.response_last_refill_at);
        limiter.response_last_refill_at = now;
        limiter.last_seen_at = now;
        limiter.response_tokens =
            (limiter.response_tokens + elapsed.as_secs_f64() * fill_rate).min(burst as f64);
        let cost = bytes.max(1) as f64;
        if limiter.response_tokens < cost {
            return false;
        }

        limiter.response_tokens -= cost;
        true
    }

    fn respond_to(
        &mut self,
        source: SocketAddr,
        response: KrpcResponseEnvelope,
        now: Instant,
    ) -> InboundAction {
        let response = response.with_observed_addr(source);
        let Ok(payload) = serde_bencode::to_bytes(&response) else {
            return InboundAction::Drop;
        };
        if !self.allow_response_bytes(source.ip(), payload.len(), now) {
            return InboundAction::Drop;
        }
        InboundAction::Respond(response)
    }

    fn error_to(
        &mut self,
        source_ip: IpAddr,
        error: KrpcErrorEnvelope,
        now: Instant,
    ) -> InboundAction {
        let Ok(payload) = serde_bencode::to_bytes(&error) else {
            return InboundAction::Drop;
        };
        if !self.allow_response_bytes(source_ip, payload.len(), now) {
            return InboundAction::Drop;
        }
        InboundAction::Error(error)
    }

    fn prune_stale_rate_limiters(&mut self, now: Instant) {
        let prune_due = match self.last_rate_limiter_prune_at {
            Some(last_prune_at) => {
                now.saturating_duration_since(last_prune_at) >= RATE_LIMITER_PRUNE_INTERVAL
            }
            None => true,
        };
        if !prune_due && self.per_ip_rate_limits.len() < MAX_RATE_LIMITER_ENTRIES {
            return;
        }

        self.per_ip_rate_limits.retain(|_, limiter| {
            now.saturating_duration_since(limiter.last_seen_at) <= RATE_LIMITER_IDLE_TTL
        });
        self.last_rate_limiter_prune_at = Some(now);
    }

    fn closest_nodes_for(
        &self,
        routing: &RoutingTable,
        target: NodeId,
        source: SocketAddr,
        now: Instant,
    ) -> Vec<CompactNode> {
        routing
            .closest_good_nodes(target, self.config.closest_nodes_limit, now)
            .into_iter()
            .filter(|record| record.addr != source)
            .filter_map(|record| {
                Some(CompactNode {
                    id: record.node_id?,
                    addr: record.addr,
                })
            })
            .collect()
    }

    fn append_requested_cross_family_nodes(
        &self,
        body: &mut KrpcResponseBody,
        routing: Option<&RoutingTable>,
        wants_family: impl Fn(AddressFamily) -> bool,
        target: NodeId,
        source: SocketAddr,
        now: Instant,
    ) {
        let Some(routing) = routing else {
            return;
        };
        let family = routing.family();
        if family == self.config.family || !wants_family(family) {
            return;
        }

        let nodes = self.closest_nodes_for(routing, target, source, now);
        if !nodes.is_empty() {
            body.set_closest_nodes(family, &nodes);
        }
    }
}

fn remember_inbound_node(
    routing: &mut RoutingTable,
    source: SocketAddr,
    node_id: NodeId,
    now: Instant,
) {
    if !routing.record_inbound_query(source, Some(node_id), now) {
        let mut record = NodeRecord::new(source, Some(node_id), now);
        record.note_inbound_query(now);
        let _ = routing.insert(record, now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::krpc::KrpcGetPeersArgs;
    use crate::dht::peer_store::PeerStoreConfig;
    use crate::dht::routing::RoutingConfig;
    use crate::dht::token::TokenConfig;
    use serde_bytes::ByteBuf;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

    fn source_ip(index: usize) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(
            10,
            ((index >> 16) & 0xff) as u8,
            ((index >> 8) & 0xff) as u8,
            (index & 0xff) as u8,
        ))
    }

    fn node_id(byte: u8) -> NodeId {
        NodeId::from([byte; NodeId::LEN])
    }

    fn info_hash(byte: u8) -> InfoHash {
        InfoHash::from([byte; InfoHash::LEN])
    }

    #[test]
    fn rate_limiter_prunes_idle_sources() {
        let start = Instant::now();
        let mut actor = InboundActor::new(InboundConfig::default());

        assert!(actor.allow_query(source_ip(1), start));
        assert!(actor.allow_query(source_ip(2), start + Duration::from_secs(1)));
        assert_eq!(actor.per_ip_rate_limits.len(), 2);

        let later = start + RATE_LIMITER_IDLE_TTL + RATE_LIMITER_PRUNE_INTERVAL;
        assert!(actor.allow_query(source_ip(3), later));

        assert_eq!(actor.per_ip_rate_limits.len(), 1);
        assert!(actor.per_ip_rate_limits.contains_key(&source_ip(3)));
    }

    #[test]
    fn rate_limiter_rejects_new_sources_at_hard_cap() {
        let start = Instant::now();
        let mut actor = InboundActor::new(InboundConfig::default());

        for index in 0..MAX_RATE_LIMITER_ENTRIES {
            assert!(actor.allow_query(source_ip(index), start));
        }

        let rejected = source_ip(MAX_RATE_LIMITER_ENTRIES);
        assert!(!actor.allow_query(rejected, start + Duration::from_secs(1)));
        assert_eq!(actor.per_ip_rate_limits.len(), MAX_RATE_LIMITER_ENTRIES);
        assert!(!actor.per_ip_rate_limits.contains_key(&rejected));
    }

    #[test]
    fn response_byte_limiter_rejects_excess_payload_bytes() {
        let start = Instant::now();
        let source = source_ip(1);
        let mut actor = InboundActor::new(InboundConfig {
            response_bytes_per_second: 10,
            response_burst_bytes: 10,
            ..InboundConfig::default()
        });

        assert!(actor.allow_response_bytes(source, 8, start));
        assert!(!actor.allow_response_bytes(source, 3, start));
        assert!(actor.allow_response_bytes(source, 3, start + Duration::from_secs(1)));
    }

    #[test]
    fn get_peers_response_includes_values_and_closest_nodes() {
        let now = Instant::now();
        let wall_clock = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let mut actor = InboundActor::new(InboundConfig::default());
        let mut routing = RoutingTable::new(
            node_id(1),
            RoutingConfig {
                family: AddressFamily::Ipv4,
                ..RoutingConfig::default()
            },
            now,
        );
        let route_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 40_001));
        let mut route = NodeRecord::new(route_addr, Some(node_id(3)), now);
        route.note_query_response(Some(node_id(3)), now);
        assert_eq!(
            routing.insert(route, now),
            crate::dht::routing::InsertOutcome::Inserted
        );

        let hash = info_hash(9);
        let mut peer_store = PeerStore::new(PeerStoreConfig::default());
        peer_store.insert(
            hash,
            CompactPeer {
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 50_001)),
            },
            wall_clock,
        );
        let mut token_service = TokenService::new(TokenConfig::default(), now);

        let action = actor.handle_query(
            InboundRequestContext {
                source: SocketAddr::from((Ipv4Addr::LOCALHOST, 60_001)),
            },
            KrpcIncomingQuery::GetPeers {
                transaction_id: ByteBuf::from(vec![1, 2, 3, 4]),
                version: None,
                args: KrpcGetPeersArgs::new(node_id(2), hash),
            },
            node_id(1),
            &mut routing,
            None,
            &mut token_service,
            &mut peer_store,
            now,
            wall_clock,
        );

        let InboundAction::Respond(response) = action else {
            panic!("expected get_peers response");
        };
        let body = response.r.expect("response body");
        assert_eq!(body.peers(AddressFamily::Ipv4).len(), 1);
        assert_eq!(body.closest_nodes(AddressFamily::Ipv4).len(), 1);
        assert!(!body.token.is_empty());
    }

    #[test]
    fn get_peers_want_includes_cross_family_nodes() {
        let now = Instant::now();
        let wall_clock = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
        let mut actor = InboundActor::new(InboundConfig::default());
        let mut ipv4_routing = RoutingTable::new(
            node_id(1),
            RoutingConfig {
                family: AddressFamily::Ipv4,
                ..RoutingConfig::default()
            },
            now,
        );
        let mut ipv6_routing = RoutingTable::new(
            node_id(1),
            RoutingConfig {
                family: AddressFamily::Ipv6,
                ..RoutingConfig::default()
            },
            now,
        );
        let route_addr = SocketAddr::from((Ipv6Addr::LOCALHOST, 40_001));
        let mut route = NodeRecord::new(route_addr, Some(node_id(4)), now);
        route.note_query_response(Some(node_id(4)), now);
        assert_eq!(
            ipv6_routing.insert(route, now),
            crate::dht::routing::InsertOutcome::Inserted
        );

        let hash = info_hash(9);
        let mut peer_store = PeerStore::new(PeerStoreConfig::default());
        let mut token_service = TokenService::new(TokenConfig::default(), now);

        let action = actor.handle_query(
            InboundRequestContext {
                source: SocketAddr::from((Ipv4Addr::LOCALHOST, 60_001)),
            },
            KrpcIncomingQuery::GetPeers {
                transaction_id: ByteBuf::from(vec![1, 2, 3, 4]),
                version: None,
                args: KrpcGetPeersArgs::new(node_id(2), hash).with_want(&[AddressFamily::Ipv6]),
            },
            node_id(1),
            &mut ipv4_routing,
            Some(&ipv6_routing),
            &mut token_service,
            &mut peer_store,
            now,
            wall_clock,
        );

        let InboundAction::Respond(response) = action else {
            panic!("expected get_peers response");
        };
        let body = response.r.expect("response body");
        assert_eq!(body.closest_nodes(AddressFamily::Ipv6).len(), 1);
    }
}
