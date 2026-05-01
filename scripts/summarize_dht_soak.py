#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2025 The superseedr Contributors
# SPDX-License-Identifier: GPL-3.0-or-later

"""Summarize DHT soak status samples and planner trace logs.

The script is intentionally read-only unless cleanup flags are provided. It
expects status samples as JSON lines matching `superseedr --json status` derived
fields, and an optional app log containing `superseedr::dht_planner` traces.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from statistics import mean
from typing import Any


START_LOOKUP_RE = re.compile(r'stage="emit" effect="start_lookup"')
PEERS_RECEIVED_RE = re.compile(r'action="peers_received"')
DRAIN_RECORDED_RE = re.compile(r'stage="emit" effect="drain_peers_recorded"')
DRAIN_FINALIZED_RE = re.compile(r'stage="apply" effect="drain_finalized"')
LOOKUP_FINISHED_RE = re.compile(r'stage="apply" effect="lookup_finished"')
LOOKUP_PARKED_RE = re.compile(r'stage="apply" effect="lookup_parked"')
PLAN_DUE_RE = re.compile(r'action="plan_due"')
METRICS_UPDATED_RE = re.compile(r'action="demand_metrics_updated"')
FIELD_RE_TEMPLATE = r"{field}=Some\((\d+)\)"
CLASS_RE = re.compile(r"demand_class=Some\(([^)]+)\)")
SLICE_CLASS_RE = re.compile(r"slice_class=Some\(([^)]+)\)")


def load_samples(path: Path) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                samples.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise SystemExit(f"{path}:{line_number}: invalid JSON: {error}") from error
    return samples


def lines_in_window(path: Path, start: str | None, end: str | None) -> list[str]:
    lines: list[str] = []
    with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
        for line in handle:
            if start is not None and line < start:
                continue
            if end is not None and line > end:
                continue
            lines.append(line.rstrip("\n"))
    return lines


def sum_field(lines: list[str], field: str) -> tuple[int, int]:
    pattern = re.compile(FIELD_RE_TEMPLATE.format(field=re.escape(field)))
    total = 0
    maximum = 0
    for line in lines:
        match = pattern.search(line)
        if match is None:
            continue
        value = int(match.group(1))
        total += value
        maximum = max(maximum, value)
    return total, maximum


def some_field(line: str, field: str) -> str | None:
    match = re.search(rf"{re.escape(field)}=Some\(([^)]*)\)", line)
    if match is None:
        return None
    return match.group(1)


def int_some_field(line: str, field: str) -> int | None:
    value = some_field(line, field)
    if value is None or not value.isdigit():
        return None
    return int(value)


def bool_some_field(line: str, field: str) -> bool | None:
    value = some_field(line, field)
    if value == "true":
        return True
    if value == "false":
        return False
    return None


def count_some_values(lines: list[str], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for line in lines:
        value = some_field(line, field)
        if value is None:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def average_int_field(lines: list[str], field: str) -> float | None:
    values = [
        value
        for line in lines
        if (value := int_some_field(line, field)) is not None
    ]
    if not values:
        return None
    return round(mean(values), 1)


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {
            "status_samples": 0,
            "sample_errors": 0,
        }

    routes = [int(sample["routes"]) for sample in samples]
    queries = [int(sample["q"]) for sample in samples]
    lookups = [int(sample["lookups"]) for sample in samples]
    bootstrap = [int(sample["bootstrap"]) for sample in samples]
    warnings = [sample for sample in samples if sample.get("warning") is not None]

    return {
        "status_samples": len(samples),
        "sample_errors": 0,
        "runtime_first": int(samples[0]["runtime_s"]),
        "runtime_last": int(samples[-1]["runtime_s"]),
        "enabled_all": all(bool(sample["enabled"]) for sample in samples),
        "routes_avg": round(mean(routes), 1),
        "routes_min": min(routes),
        "routes_max": max(routes),
        "q_avg": round(mean(queries), 1),
        "q_max": max(queries),
        "q_last": queries[-1],
        "lookups_avg": round(mean(lookups), 1),
        "lookups_max": max(lookups),
        "bootstrap_min": min(bootstrap),
        "bootstrap_last": bootstrap[-1],
        "status_warnings": len(warnings),
    }


def summarize_log(lines: list[str]) -> dict[str, Any]:
    planner = [line for line in lines if "superseedr::dht_planner" in line]
    actor = [line for line in lines if "superseedr::dht_actor" in line]
    starts = [line for line in planner if START_LOOKUP_RE.search(line)]
    plan_due = [line for line in planner if PLAN_DUE_RE.search(line)]
    metrics_updates = [line for line in planner if METRICS_UPDATED_RE.search(line)]

    classes = {
        "AwaitingMetadata": 0,
        "NoConnectedPeers": 0,
        "RoutineRefresh": 0,
        "Other": 0,
    }
    for line in starts:
        class_match = CLASS_RE.search(line) or SLICE_CLASS_RE.search(line)
        class_name = class_match.group(1) if class_match else "Other"
        classes[class_name if class_name in classes else "Other"] += 1

    peer_actions = [line for line in planner if PEERS_RECEIVED_RE.search(line)]
    drain_recorded = [line for line in planner if DRAIN_RECORDED_RE.search(line)]
    drain_finalized = [line for line in planner if DRAIN_FINALIZED_RE.search(line)]
    lookup_finished = [line for line in planner if LOOKUP_FINISHED_RE.search(line)]
    lookup_parked = [line for line in planner if LOOKUP_PARKED_RE.search(line)]
    peers_delivered, peers_delivered_max = sum_field(peer_actions, "peer_count")
    drain_unique_added, drain_unique_added_max = sum_field(drain_recorded, "unique_added")
    drain_finalized_unique, _ = sum_field(drain_finalized, "unique_peers")
    natural_finish_unique, _ = sum_field(lookup_finished, "unique_peers")
    parked_unique, _ = sum_field(lookup_parked, "unique_peers")
    start_cap_total, start_cap_max = sum_field(starts, "plan_unique_peer_cap")

    zero_activity_metrics = sum(
        int_some_field(line, "metrics_activity") == 0 for line in metrics_updates
    )
    accepting_metrics = sum(
        bool_some_field(line, "metrics_accepting_new_peers") is True
        for line in metrics_updates
    )
    extended_routine_metrics = sum(
        bool_some_field(line, "metrics_wants_extended_routine") is True
        for line in metrics_updates
    )
    idle_probe_starts = sum(
        bool_some_field(line, "metrics_wants_idle_probe") is True for line in starts
    )
    extended_routine_starts = sum(
        bool_some_field(line, "metrics_wants_extended_routine") is True
        for line in starts
    )

    return {
        "run_lines": len(lines),
        "actor_lines": len(actor),
        "planner_lines": len(planner),
        "selected_launches": len(starts),
        "awaiting_metadata": classes["AwaitingMetadata"],
        "no_peer": classes["NoConnectedPeers"],
        "routine": classes["RoutineRefresh"],
        "other_launch": classes["Other"],
        "launch_failures": sum("launch_failed" in line for line in planner),
        "launch_skipped": sum("launch_skipped" in line for line in planner),
        "peer_batches_dropped": sum("drop_batch" in line for line in planner),
        "peers_received_events": len(peer_actions),
        "peers_delivered": peers_delivered,
        "peers_delivered_max_batch": peers_delivered_max,
        "drain_peers_recorded_events": len(drain_recorded),
        "drain_unique_added": drain_unique_added,
        "drain_unique_added_max": drain_unique_added_max,
        "drain_finalized_events": len(drain_finalized),
        "drain_finalized_unique_sum": drain_finalized_unique,
        "natural_finish_events": len(lookup_finished),
        "natural_finish_unique_sum": natural_finish_unique,
        "parked_events": len(lookup_parked),
        "parked_unique_sum": parked_unique,
        "selection_reasons": count_some_values(starts, "selection_reason"),
        "power_multipliers": count_some_values(starts, "plan_power_multiplier"),
        "stop_reasons": count_some_values(lookup_parked, "stop_reason"),
        "finish_modes": count_some_values(drain_finalized, "finish_mode"),
        "start_unique_peer_cap_avg": average_int_field(starts, "plan_unique_peer_cap"),
        "start_unique_peer_cap_max": start_cap_max,
        "start_unique_peer_cap_total": start_cap_total,
        "start_wall_time_ms_avg": average_int_field(starts, "plan_max_wall_time_ms"),
        "start_idle_timeout_ms_avg": average_int_field(starts, "plan_idle_timeout_ms"),
        "extended_routine_starts": extended_routine_starts,
        "idle_probe_wanted_starts": idle_probe_starts,
        "plan_due_events": len(plan_due),
        "plan_due_total_avg": average_int_field(plan_due, "plan_due_total"),
        "plan_launch_budget_avg": average_int_field(plan_due, "plan_launch_budget"),
        "plan_throttled_awaiting_sum": sum_field(plan_due, "plan_throttled_awaiting")[0],
        "plan_throttled_no_peers_sum": sum_field(plan_due, "plan_throttled_no_peers")[0],
        "plan_throttled_routine_sum": sum_field(plan_due, "plan_throttled_routine")[0],
        "plan_idle_probe_active_events": sum(
            bool_some_field(line, "plan_idle_probe_active") is True for line in plan_due
        ),
        "planner_idle_probe_multipliers": count_some_values(
            plan_due, "planner_idle_probe_multiplier"
        ),
        "metrics_update_events": len(metrics_updates),
        "metrics_zero_activity_events": zero_activity_metrics,
        "metrics_accepting_new_peers_events": accepting_metrics,
        "metrics_extended_routine_events": extended_routine_metrics,
        "errors": sum("ERROR" in line for line in lines),
        "warnings": sum(" WARN " in line for line in lines),
        "service_actor": sum('domain="service"' in line for line in actor),
        "lifecycle_actor": sum('domain="lifecycle"' in line for line in actor),
        "demand_actor": sum('domain="demand_command"' in line for line in actor),
        "runtime_actor": sum('domain="runtime_command"' in line for line in actor),
    }


def cleanup(args: argparse.Namespace) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if args.trim_log_to_length is not None:
        if args.log is None:
            raise SystemExit("--trim-log-to-length requires --log")
        before = args.log.stat().st_size if args.log.exists() else 0
        with args.log.open("r+b") as handle:
            handle.truncate(args.trim_log_to_length)
        result["log_bytes_before_cleanup"] = before
        result["log_bytes_after_cleanup"] = args.log.stat().st_size
        result["log_bytes_removed"] = before - result["log_bytes_after_cleanup"]

    removed: list[str] = []
    for path in args.delete:
        if path.exists():
            path.unlink()
            removed.append(str(path))
    result["deleted_files"] = removed
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=Path, help="JSONL status sample file")
    parser.add_argument("--log", type=Path, help="app log containing DHT traces")
    parser.add_argument("--start", help="inclusive ISO timestamp prefix for log window")
    parser.add_argument("--end", help="exclusive ISO timestamp prefix for log window")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument(
        "--trim-log-to-length",
        type=int,
        help="truncate --log back to this byte length after summarizing",
    )
    parser.add_argument(
        "--delete",
        type=Path,
        action="append",
        default=[],
        help="delete generated artifact after summarizing; can be repeated",
    )
    parser.add_argument(
        "--assert-min-peers",
        type=int,
        help="fail if parsed planner traces delivered fewer peers than this",
    )
    parser.add_argument(
        "--assert-max-q-avg",
        type=float,
        help="fail if sampled average DHT query pressure is above this value",
    )
    parser.add_argument(
        "--assert-no-launch-failures",
        action="store_true",
        help="fail if planner traces contain launch failures, skipped launches, or dropped peer batches",
    )
    return parser.parse_args()


def assert_thresholds(summary: dict[str, Any], args: argparse.Namespace) -> None:
    failures: list[str] = []
    if args.assert_min_peers is not None:
        peers_delivered = summary.get("peers_delivered")
        if peers_delivered is None:
            failures.append("--assert-min-peers requires --log planner traces")
        elif peers_delivered < args.assert_min_peers:
            failures.append(
                f"peers_delivered {peers_delivered} < {args.assert_min_peers}"
            )

    if args.assert_max_q_avg is not None:
        q_avg = summary.get("q_avg")
        if q_avg is None:
            failures.append("--assert-max-q-avg requires --samples")
        elif q_avg > args.assert_max_q_avg:
            failures.append(f"q_avg {q_avg} > {args.assert_max_q_avg}")

    if args.assert_no_launch_failures:
        for field in ("launch_failures", "launch_skipped", "peer_batches_dropped"):
            value = summary.get(field)
            if value is None:
                failures.append(f"--assert-no-launch-failures requires {field} from --log")
            elif value:
                failures.append(f"{field} is {value}")

    if failures:
        raise SystemExit("DHT soak threshold failure: " + "; ".join(failures))


def main() -> None:
    args = parse_args()
    summary: dict[str, Any] = {}

    if args.samples is not None:
        summary.update(summarize_samples(load_samples(args.samples)))
    if args.log is not None:
        summary.update(summarize_log(lines_in_window(args.log, args.start, args.end)))
    if args.trim_log_to_length is not None or args.delete:
        summary["cleanup"] = cleanup(args)

    assert_thresholds(summary, args)

    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
        return

    print(f"Status samples: {summary.get('status_samples', 0)}")
    if "runtime_last" in summary:
        print(
            "Runtime: "
            f"{summary['runtime_first']}s..{summary['runtime_last']}s, "
            f"enabled_all={summary['enabled_all']}"
        )
        print(
            "Routes: "
            f"avg {summary['routes_avg']}, "
            f"range {summary['routes_min']}..{summary['routes_max']}"
        )
        print(
            "Query pressure: "
            f"avg {summary['q_avg']}, max {summary['q_max']}, last {summary['q_last']}"
        )
    if "selected_launches" in summary:
        launches = summary["selected_launches"]
        print(f"Selected launches: {launches}")
        if launches:
            print(
                "Launch mix: "
                f"{summary['no_peer'] / launches:.1%} no-peer, "
                f"{summary['routine'] / launches:.1%} routine, "
                f"{summary['awaiting_metadata'] / launches:.1%} awaiting-metadata"
            )
        print(
            "Failures/skips/drops: "
            f"{summary['launch_failures']}/"
            f"{summary['launch_skipped']}/"
            f"{summary['peer_batches_dropped']}"
        )
        print(f"Peers delivered: {summary['peers_delivered']}")
        print(f"Drain unique added: {summary['drain_unique_added']}")
        print(
            "Planner pressure: "
            f"{summary['plan_due_events']} plan ticks, "
            f"due avg {summary['plan_due_total_avg']}, "
            f"budget avg {summary['plan_launch_budget_avg']}, "
            f"throttled A/N/R "
            f"{summary['plan_throttled_awaiting_sum']}/"
            f"{summary['plan_throttled_no_peers_sum']}/"
            f"{summary['plan_throttled_routine_sum']}"
        )
        print(
            "Start plan: "
            f"multipliers {summary['power_multipliers']}, "
            f"reasons {summary['selection_reasons']}, "
            f"cap avg/max {summary['start_unique_peer_cap_avg']}/"
            f"{summary['start_unique_peer_cap_max']}"
        )
        print(
            "Stops/yield: "
            f"natural unique {summary['natural_finish_unique_sum']}, "
            f"parked unique {summary['parked_unique_sum']}, "
            f"drain finalized unique {summary['drain_finalized_unique_sum']}, "
            f"stop reasons {summary['stop_reasons']}"
        )
        print(
            "Demand metrics: "
            f"{summary['metrics_update_events']} updates, "
            f"zero-activity {summary['metrics_zero_activity_events']}, "
            f"accepting {summary['metrics_accepting_new_peers_events']}, "
            f"extended-routine {summary['metrics_extended_routine_events']}"
        )
        print(f"Trace errors/warnings: {summary['errors']}/{summary['warnings']}")
    if "cleanup" in summary:
        print(f"Cleanup: {json.dumps(summary['cleanup'], sort_keys=True)}")


if __name__ == "__main__":
    main()
