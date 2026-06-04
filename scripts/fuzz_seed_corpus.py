#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025 The superseedr Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "fuzz" / "corpus"


def hx(value: str) -> bytes:
    return bytes.fromhex(value)


def asc(value: str) -> bytes:
    return value.encode("ascii")


SEEDS: dict[str, dict[str, bytes]] = {
    "utp_packet_decode": {
        "syn": hx("4100000100000001000000000004000000010000"),
        "state": hx("2100000100000002000000010004000000020001"),
        "data_payload": hx("010000020000000300000001000400000003000268656c6c6f"),
        "state_sack": hx("2101000100000004000000010004000000040003000401000000"),
    },
    "utp_packet_roundtrip": {
        "empty": b"",
        "small": hx("000102030405060708090a0b0c0d0e0f"),
        "sack_payload": hx("03040102030405060708090a0b0c0d0e0f10111213141516"),
    },
    "tcp_message_parse": {
        "keepalive": hx("00000000"),
        "choke": hx("0000000100"),
        "have": hx("000000050400000001"),
        "request": hx("0000000d06000000010000400000004000"),
        "extended": hx("00000006140068656c6c6f"),
    },
    "tcp_message_roundtrip": {
        "small": hx("00010203040506070809"),
        "requestish": hx("07000000010000400000004000"),
        "payload": hx("08000000010000400068656c6c6f776f726c64"),
    },
    "torrent_file_parse": {
        "minimal_v1": asc(
            "d8:announce24:http://tracker.invalid/a4:infod6:lengthi4e4:name4:seed"
            "12:piece lengthi16384e6:pieces20:aaaaaaaaaaaaaaaaaaaaee"
        ),
        "minimal_private": asc(
            "d4:infod6:lengthi8e4:name8:seed.bin12:piece lengthi16384e"
            "6:pieces20:bbbbbbbbbbbbbbbbbbbb7:privatei1eee"
        ),
    },
    "torrent_info_parse": {
        "info_v1": asc(
            "d6:lengthi4e4:name4:seed12:piece lengthi16384e"
            "6:pieces20:aaaaaaaaaaaaaaaaaaaae"
        ),
        "info_files": asc(
            "d5:filesld6:lengthi3e4:pathl8:part.binee4:name4:seed"
            "12:piece lengthi16384e6:pieces20:bbbbbbbbbbbbbbbbbbbbe"
        ),
    },
    "krpc_message_decode": {
        "ping": asc("d1:ad2:id20:aaaaaaaaaaaaaaaaaaaae1:q4:ping1:t4:txid1:y1:qe"),
        "get_peers": asc(
            "d1:ad2:id20:aaaaaaaaaaaaaaaaaaaa9:info_hash20:bbbbbbbbbbbbbbbbbbbb"
            "e1:q9:get_peers1:t4:txid1:y1:qe"
        ),
        "error": asc("d1:eli201e11:bad requeste1:t4:txid1:y1:ee"),
    },
    "krpc_compact_decode": {
        "ipv4_peer": hx("007f0000011ae1"),
        "ipv6_peer": hx("01000000000000000000000000000000000000011ae1"),
        "ipv4_node": hx("00" + "01" * 20 + "7f0000011ae1"),
    },
    "krpc_query_roundtrip": {
        "empty": b"",
        "mixed": hx("00010203" + "01" * 20 + "02" * 20 + "03" * 20 + "030104d2"),
    },
    "dht_lifecycle_reduce": {
        "empty": b"",
        "bootstrap_due": hx("0100000000000000000000000000"),
        "warning": asc("seed warning lifecycle retry"),
    },
}


def main() -> None:
    for target, entries in SEEDS.items():
        target_dir = CORPUS / target
        target_dir.mkdir(parents=True, exist_ok=True)
        for name, payload in entries.items():
            (target_dir / name).write_bytes(payload)


if __name__ == "__main__":
    main()
