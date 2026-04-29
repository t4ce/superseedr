// SPDX-FileCopyrightText: 2025 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use super::types::{is_routable_dht_addr, AddressFamily};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

const PUBLIC_ADDRESS_QUORUM: usize = 3;
const MAX_PUBLIC_ADDRESS_CANDIDATES: usize = 64;

#[derive(Debug, Clone, Default)]
pub struct PublicAddressObserver {
    votes: HashMap<SocketAddr, HashSet<SocketAddr>>,
    confirmed_ipv4: Option<SocketAddr>,
    confirmed_ipv6: Option<SocketAddr>,
}

impl PublicAddressObserver {
    pub fn record_observation(
        &mut self,
        voter: SocketAddr,
        observed: SocketAddr,
    ) -> Option<SocketAddr> {
        if AddressFamily::for_addr(voter) != AddressFamily::for_addr(observed)
            || !is_routable_dht_addr(voter)
            || !is_routable_dht_addr(observed)
        {
            return self.confirmed_for(AddressFamily::for_addr(observed));
        }

        if !self.votes.contains_key(&observed) && self.votes.len() >= MAX_PUBLIC_ADDRESS_CANDIDATES
        {
            self.prune_weakest_candidate();
        }

        let voters = self.votes.entry(observed).or_default();
        voters.insert(voter);
        if voters.len() >= PUBLIC_ADDRESS_QUORUM {
            match AddressFamily::for_addr(observed) {
                AddressFamily::Ipv4 => self.confirmed_ipv4 = Some(observed),
                AddressFamily::Ipv6 => self.confirmed_ipv6 = Some(observed),
            }
        }

        self.confirmed_for(AddressFamily::for_addr(observed))
    }

    pub fn confirmed_for(&self, family: AddressFamily) -> Option<SocketAddr> {
        match family {
            AddressFamily::Ipv4 => self.confirmed_ipv4,
            AddressFamily::Ipv6 => self.confirmed_ipv6,
        }
    }

    fn prune_weakest_candidate(&mut self) {
        let Some(candidate) = self
            .votes
            .iter()
            .min_by_key(|(_, voters)| voters.len())
            .map(|(candidate, _)| *candidate)
        else {
            return;
        };
        self.votes.remove(&candidate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr};

    fn addr(octet: u8, port: u16) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::new(127, 0, 0, octet), port))
    }

    #[test]
    fn public_address_requires_quorum() {
        let mut observer = PublicAddressObserver::default();
        let observed = addr(10, 6881);

        assert_eq!(observer.record_observation(addr(1, 1001), observed), None);
        assert_eq!(observer.record_observation(addr(2, 1002), observed), None);
        assert_eq!(
            observer.record_observation(addr(3, 1003), observed),
            Some(observed)
        );
        assert_eq!(observer.confirmed_for(AddressFamily::Ipv4), Some(observed));
    }

    #[test]
    fn duplicate_voter_does_not_satisfy_quorum() {
        let mut observer = PublicAddressObserver::default();
        let observed = addr(10, 6881);
        let voter = addr(1, 1001);

        assert_eq!(observer.record_observation(voter, observed), None);
        assert_eq!(observer.record_observation(voter, observed), None);
        assert_eq!(observer.confirmed_for(AddressFamily::Ipv4), None);
    }
}
