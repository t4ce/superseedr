// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::routing::RoutingSnapshot;
use super::types::{AddressFamily, Bep42State, NodeId, NodeRecord, NodeTrust};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const PERSISTENCE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistenceConfig {
    pub path: PathBuf,
    pub max_age: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRoutingNode {
    pub addr: SocketAddr,
    pub node_id: Option<NodeId>,
    pub trust: NodeTrust,
    pub bep42_state: Bep42State,
    pub consecutive_failures: u16,
    pub dead_referral_count: u16,
    pub live_referral_count: u16,
    pub id_churn_count: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRoutingTable {
    pub family: AddressFamily,
    pub nodes: Vec<PersistedRoutingNode>,
    #[serde(default)]
    pub replacements: Vec<PersistedRoutingNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedStateEnvelope {
    pub version: u32,
    pub created_at_unix_secs: u64,
    pub node_id: NodeId,
    pub ipv4_routes: PersistedRoutingTable,
    pub ipv6_routes: PersistedRoutingTable,
}

#[derive(Debug, Clone)]
pub struct PersistenceManager {
    config: PersistenceConfig,
}

impl PersistenceManager {
    pub fn new(config: PersistenceConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    pub fn build_snapshot(
        &self,
        node_id: NodeId,
        ipv4_routes: &RoutingSnapshot,
        ipv6_routes: &RoutingSnapshot,
        now: SystemTime,
    ) -> PersistedStateEnvelope {
        PersistedStateEnvelope {
            version: PERSISTENCE_VERSION,
            created_at_unix_secs: now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
            node_id,
            ipv4_routes: PersistedRoutingTable {
                family: AddressFamily::Ipv4,
                nodes: ipv4_routes
                    .nodes
                    .iter()
                    .map(PersistedRoutingNode::from_record)
                    .collect(),
                replacements: Vec::new(),
            },
            ipv6_routes: PersistedRoutingTable {
                family: AddressFamily::Ipv6,
                nodes: ipv6_routes
                    .nodes
                    .iter()
                    .map(PersistedRoutingNode::from_record)
                    .collect(),
                replacements: Vec::new(),
            },
        }
    }

    pub fn save_snapshot(&self, snapshot: &PersistedStateEnvelope) -> io::Result<()> {
        let new_total = snapshot.ipv4_routes.nodes.len() + snapshot.ipv6_routes.nodes.len();
        if new_total == 0 {
            return Ok(());
        }

        if let Some(existing) = self.load_snapshot(SystemTime::now())? {
            let existing_total =
                existing.ipv4_routes.nodes.len() + existing.ipv6_routes.nodes.len();
            if existing.node_id == snapshot.node_id && existing_total > new_total && new_total < 16
            {
                return Ok(());
            }
        }

        if let Some(parent) = self.config.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(snapshot)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        fs::write(&self.config.path, bytes)
    }

    pub fn load_snapshot(&self, now: SystemTime) -> io::Result<Option<PersistedStateEnvelope>> {
        let bytes = match fs::read(&self.config.path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };

        let snapshot = match serde_json::from_slice::<PersistedStateEnvelope>(&bytes) {
            Ok(snapshot) => snapshot,
            Err(_) => return Ok(None),
        };

        if snapshot.version != PERSISTENCE_VERSION {
            return Ok(None);
        }

        let created_at = UNIX_EPOCH + Duration::from_secs(snapshot.created_at_unix_secs);
        if now.duration_since(created_at).unwrap_or_default() > self.config.max_age {
            return Ok(None);
        }

        Ok(Some(snapshot))
    }

    pub fn restore_nodes(&self, routes: &PersistedRoutingTable, now: Instant) -> Vec<NodeRecord> {
        routes
            .nodes
            .iter()
            .map(|node| node.to_record(now))
            .collect()
    }
}

impl PersistedRoutingNode {
    fn from_record(record: &NodeRecord) -> Self {
        Self {
            addr: record.addr,
            node_id: record.node_id,
            trust: record.trust,
            bep42_state: record.bep42_state,
            consecutive_failures: record.consecutive_failures,
            dead_referral_count: record.dead_referral_count,
            live_referral_count: record.live_referral_count,
            id_churn_count: record.id_churn_count,
        }
    }

    fn to_record(&self, now: Instant) -> NodeRecord {
        let mut record = NodeRecord::new(self.addr, self.node_id, now);
        record.trust = normalize_persisted_trust(self.trust);
        record.bep42_state = self.bep42_state;
        record.consecutive_failures = self.consecutive_failures;
        record.dead_referral_count = self.dead_referral_count;
        record.live_referral_count = self.live_referral_count;
        record.id_churn_count = self.id_churn_count;
        record
    }
}

fn normalize_persisted_trust(trust: NodeTrust) -> NodeTrust {
    match trust {
        NodeTrust::Suspicious => NodeTrust::Neutral,
        trust => trust,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    fn persisted_node(
        octet: u8,
        trust: NodeTrust,
        bep42_state: Bep42State,
        id_churn_count: u16,
    ) -> PersistedRoutingNode {
        PersistedRoutingNode {
            addr: SocketAddr::from((Ipv4Addr::new(203, 0, 113, octet), 6881)),
            node_id: Some(NodeId::from([8; NodeId::LEN])),
            trust,
            bep42_state,
            consecutive_failures: 0,
            dead_referral_count: 0,
            live_referral_count: 1,
            id_churn_count,
        }
    }

    #[test]
    fn restore_nodes_neutralizes_stale_suspicious_trust() {
        let node = persisted_node(10, NodeTrust::Suspicious, Bep42State::NonCompliant, 1);
        let restored = node.to_record(Instant::now());

        assert_eq!(restored.trust, NodeTrust::Neutral);
        assert_eq!(restored.bep42_state, Bep42State::NonCompliant);
        assert_eq!(restored.id_churn_count, 1);
    }

    #[test]
    fn restore_nodes_preserves_trusted_routes() {
        let node = persisted_node(11, NodeTrust::Trusted, Bep42State::Compliant, 0);
        let restored = node.to_record(Instant::now());

        assert_eq!(restored.trust, NodeTrust::Trusted);
    }

    #[test]
    fn restore_nodes_normalizes_mixed_legacy_route_trust() {
        let manager = PersistenceManager::new(PersistenceConfig {
            path: PathBuf::from("unused-dht-state.json"),
            max_age: Duration::from_secs(60),
        });
        let routes = PersistedRoutingTable {
            family: AddressFamily::Ipv4,
            nodes: vec![
                persisted_node(20, NodeTrust::Suspicious, Bep42State::NonCompliant, 0),
                persisted_node(21, NodeTrust::Suspicious, Bep42State::Unknown, 2),
                persisted_node(22, NodeTrust::Neutral, Bep42State::Compliant, 0),
                persisted_node(23, NodeTrust::Trusted, Bep42State::Compliant, 0),
            ],
            replacements: vec![persisted_node(
                24,
                NodeTrust::Suspicious,
                Bep42State::NonCompliant,
                4,
            )],
        };

        let restored = manager.restore_nodes(&routes, Instant::now());

        assert_eq!(
            restored
                .iter()
                .map(|record| record.trust)
                .collect::<Vec<_>>(),
            vec![
                NodeTrust::Neutral,
                NodeTrust::Neutral,
                NodeTrust::Neutral,
                NodeTrust::Trusted
            ]
        );
        assert_eq!(restored[0].bep42_state, Bep42State::NonCompliant);
        assert_eq!(restored[1].id_churn_count, 2);
    }

    #[test]
    fn load_snapshot_ignores_invalid_stale_and_unsupported_files() {
        let temp_dir = tempfile::tempdir().expect("temp dht persistence dir");
        let path = temp_dir.path().join("dht_state.json");
        let manager = PersistenceManager::new(PersistenceConfig {
            path: path.clone(),
            max_age: Duration::from_secs(60),
        });

        fs::write(&path, b"{not json").expect("write invalid state");
        assert!(manager
            .load_snapshot(SystemTime::now())
            .expect("load invalid")
            .is_none());

        let mut snapshot = PersistedStateEnvelope {
            version: PERSISTENCE_VERSION + 1,
            created_at_unix_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            node_id: NodeId::from([1; NodeId::LEN]),
            ipv4_routes: PersistedRoutingTable {
                family: AddressFamily::Ipv4,
                nodes: vec![persisted_node(
                    30,
                    NodeTrust::Neutral,
                    Bep42State::Unknown,
                    0,
                )],
                replacements: Vec::new(),
            },
            ipv6_routes: PersistedRoutingTable {
                family: AddressFamily::Ipv6,
                nodes: Vec::new(),
                replacements: Vec::new(),
            },
        };
        fs::write(
            &path,
            serde_json::to_vec(&snapshot).expect("serialize unsupported snapshot"),
        )
        .expect("write unsupported snapshot");
        assert!(manager
            .load_snapshot(SystemTime::now())
            .expect("load unsupported")
            .is_none());

        snapshot.version = PERSISTENCE_VERSION;
        snapshot.created_at_unix_secs = 1;
        fs::write(
            &path,
            serde_json::to_vec(&snapshot).expect("serialize stale snapshot"),
        )
        .expect("write stale snapshot");
        assert!(manager
            .load_snapshot(UNIX_EPOCH + Duration::from_secs(10_000))
            .expect("load stale")
            .is_none());
    }
}
