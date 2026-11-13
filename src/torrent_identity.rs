// SPDX-FileCopyrightText: 2026 The superseedr Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

use data_encoding::BASE32;
use magnet_url::Magnet;
#[cfg(not(feature = "trueos-blueprint"))]
use sha1::Digest;
#[cfg(not(feature = "trueos-blueprint"))]
use sha2::Sha256;
use std::path::Path;

#[cfg(not(feature = "trueos-blueprint"))]
use crate::torrent_file::parser::from_bytes;

pub fn decode_info_hash(hash_string: &str) -> Result<Vec<u8>, String> {
    if let Ok(bytes) = hex::decode(hash_string) {
        if bytes.len() == 20 {
            return Ok(bytes);
        }
        if bytes.len() == 34 && bytes[0] == 0x12 && bytes[1] == 0x20 {
            return Ok(bytes[2..22].to_vec());
        }
    }

    if let Ok(bytes) = BASE32.decode(hash_string.to_uppercase().as_bytes()) {
        if bytes.len() == 20 {
            return Ok(bytes);
        }
        if bytes.len() == 34 && bytes[0] == 0x12 && bytes[1] == 0x20 {
            return Ok(bytes[2..22].to_vec());
        }
    }

    Err(format!("Invalid info_hash format/length: {}", hash_string))
}

pub fn parse_hybrid_hashes(magnet_link: &str) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let query = magnet_link
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or(magnet_link);
    let mut v1: Option<Vec<u8>> = None;
    let mut v2: Option<Vec<u8>> = None;

    for part in query.split('&') {
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        if !key.eq_ignore_ascii_case("xt") {
            continue;
        }

        const BTIH_PREFIX: &str = "urn:btih:";
        const BTMH_PREFIX: &str = "urn:btmh:";
        if value.len() > BTIH_PREFIX.len()
            && value
                .get(..BTIH_PREFIX.len())
                .is_some_and(|p| p.eq_ignore_ascii_case(BTIH_PREFIX))
        {
            v1 = value
                .get(BTIH_PREFIX.len()..)
                .and_then(|h| decode_info_hash(h).ok());
        } else if value.len() > BTMH_PREFIX.len()
            && value
                .get(..BTMH_PREFIX.len())
                .is_some_and(|p| p.eq_ignore_ascii_case(BTMH_PREFIX))
        {
            v2 = value
                .get(BTMH_PREFIX.len()..)
                .and_then(|h| decode_info_hash(h).ok());
        }
    }

    (v1, v2)
}

pub fn canonical_info_hash_from_magnet_link(magnet_link: &str) -> Option<Vec<u8>> {
    let (v1_hash, v2_hash) = parse_hybrid_hashes(magnet_link);
    if v1_hash.is_some() || v2_hash.is_some() {
        return v1_hash.or(v2_hash);
    }

    Magnet::new(magnet_link)
        .ok()
        .and_then(|magnet| magnet.hash().map(str::to_string))
        .and_then(|hash| decode_info_hash(&hash).ok())
}

pub fn info_hash_from_torrent_source(source: &str) -> Option<Vec<u8>> {
    if source.starts_with("magnet:") {
        canonical_info_hash_from_magnet_link(source)
    } else {
        Path::new(source)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|stem| hex::decode(stem).ok())
    }
}

#[cfg(not(feature = "trueos-blueprint"))]
pub fn info_hash_from_torrent_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let torrent = from_bytes(bytes).ok()?;

    let hash = if torrent.info.meta_version == Some(2) {
        if !torrent.info.pieces.is_empty() {
            let mut hasher = sha1::Sha1::new();
            hasher.update(&torrent.info_dict_bencode);
            hasher.finalize().to_vec()
        } else {
            let mut hasher = Sha256::new();
            hasher.update(&torrent.info_dict_bencode);
            hasher.finalize()[0..20].to_vec()
        }
    } else {
        let mut hasher = sha1::Sha1::new();
        hasher.update(&torrent.info_dict_bencode);
        hasher.finalize().to_vec()
    };

    Some(hash)
}

#[cfg(test)]
mod tests {
    use super::{canonical_info_hash_from_magnet_link, decode_info_hash, parse_hybrid_hashes};

    #[test]
    fn canonical_magnet_identity_prefers_btih_even_when_btmh_is_last() {
        let magnet = concat!(
            "magnet:?xt=urn:btih:1111111111111111111111111111111111111111",
            "&xt=urn:btmh:1220aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        assert_eq!(
            canonical_info_hash_from_magnet_link(magnet),
            Some(vec![0x11; 20])
        );
    }

    #[test]
    fn parse_hybrid_hashes_still_preserves_v2_when_v1_is_missing() {
        let magnet =
            "magnet:?xt=urn:btmh:1220aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let (v1, v2) = parse_hybrid_hashes(magnet);
        assert!(v1.is_none());
        assert_eq!(v2, Some(vec![0xaa; 20]));
    }

    #[test]
    fn decode_info_hash_accepts_v2_multihash_hex() {
        let hash = "1220aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        assert_eq!(decode_info_hash(hash), Ok(vec![0xaa; 20]));
    }
}
