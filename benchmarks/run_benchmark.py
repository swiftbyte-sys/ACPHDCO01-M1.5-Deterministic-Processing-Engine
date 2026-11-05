#!/usr/bin/env python3
"""Open-loop durable group-commit benchmark with loss injection and recovery audit."""
import argparse
import concurrent.futures
import json
import logging
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from acp_engine import ProcessingEngine
from acp_engine.transport import deliver_with_retries
from acp_engine.workload import make_operation


def percentile(values, p):
    if not values:
        return 0.0
    xs = sorted(values)
    position = (len(xs) - 1) * p
    lower = int(position)
    upper = min(lower + 1, len(xs) - 1)
    return xs[lower] + (xs[upper] - xs[lower]) * (position - lower)


def apply_expected_state(operations):
    state = {}
    for operation in operations:
        key = operation["key"]
        if operation["action"] == "set":
            state[key] = float(operation["value"])
        else:
            state[key] = state.get(key, 0.0) + float(operation["value"])
    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--target-rate", type=float, default=10000)
    parser.add_argument("--drop-rate", type=float, default=0.30)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--max-attempts", type=int, default=100)
    parser.add_argument("--output", default="benchmark-results.json")
    parser.add_argument("--wal", default=None)
    args = parser.parse_args()
    if min(args.ops, args.workers, args.batch_size, args.max_attempts) <= 0:
        parser.error("--ops, --workers, --batch-size, and --max-attempts must be positive")
    if args.target_rate <= 0 or not 0 <= args.drop_rate < 1:
        parser.error("--target-rate must be positive and --drop-rate must be in [0, 1)")

    logging.basicConfig(level=logging.WARNING)
    wal_path = Path(args.wal or tempfile.mktemp(prefix="acp-m55-", suffix=".wal"))
    wal_path.parent.mkdir(parents=True, exist_ok=True)
    if wal_path.exists() and wal_path.stat().st_size:
        parser.error("WAL already exists and is non-empty; use a fresh path: " + str(wal_path))
    engine = ProcessingEngine(wal_path, fsync=True)

    latency_us = []
    offered_operations = []
    delivered_operations = []
    committed_operations = []
    delivery_failures = 0
    processing_failures = 0
    total_attempts = 0
    total_drops = 0
    worker_exceptions = []
    processing_errors = []
    cpu_start = time.process_time()
    start = time.perf_counter()

    def run_batch(batch):
        batch_start_ns = [time.perf_counter_ns() for _ in batch]
        delivered = []
        attempts_for_batch = 0
        drops_for_batch = 0
        failed_delivery_ids = []
        for index, operation in batch:
            try:
                transport_result = deliver_with_retries(
                    operation, lambda item: item,
                    drop_rate=args.drop_rate,
                    max_attempts=args.max_attempts,
                    seed=index,
                )
                attempts = int(transport_result["delivery_attempts"])
                attempts_for_batch += attempts
                drops_for_batch += attempts - 1
                delivered.append(operation)
            except Exception:
                attempts_for_batch += args.max_attempts
                drops_for_batch += args.max_attempts
                failed_delivery_ids.append(operation["op_id"])

        processing_error = None
        if delivered:
            try:
                engine.process_many(delivered)
            except Exception as exc:
                processing_error = repr(exc)

        finished_ns = time.perf_counter_ns()
        return {
            "latencies": [(finished_ns - value) / 1000 for value in batch_start_ns],
            "offered": [operation for _, operation in batch],
            "delivered": delivered,
            "failed_delivery_ids": failed_delivery_ids,
            "attempts": attempts_for_batch,
            "drops": drops_for_batch,
            "processing_error": processing_error,
        }

    batches = []
    for offset in range(0, args.ops, args.batch_size):
        batch = [
            (index, make_operation(index))
            for index in range(offset, min(offset + args.batch_size, args.ops))
        ]
        batches.append((offset, batch))

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for offset, batch in batches:
            due_time = start + offset / args.target_rate
            delay = due_time - time.perf_counter()
            if delay > 0:
                time.sleep(delay)
            futures.append(executor.submit(run_batch, batch))

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
            except Exception as exc:
                worker_exceptions.append(repr(exc))
                continue
            latency_us.extend(result["latencies"])
            offered_operations.extend(result["offered"])
            total_attempts += result["attempts"]
            total_drops += result["drops"]
            delivery_failures += len(result["failed_delivery_ids"])
            delivered_operations.extend(result["delivered"])
            if result["processing_error"]:
                processing_failures += len(result["delivered"])
                processing_errors.append(result["processing_error"])
            else:
                committed_operations.extend(result["delivered"])

    elapsed = time.perf_counter() - start
    cpu_seconds = time.process_time() - cpu_start
    live_snapshot = engine.snapshot()
    engine.close()

    # Reopen the same WAL in a fresh engine to test actual recovery, not just
    # in-memory consistency. The expected state is derived only from batches
    # whose process_many call returned successfully (acknowledged commits).
    recovered_engine = ProcessingEngine(wal_path, fsync=True)
    recovered_snapshot = recovered_engine.snapshot()
    expected_state = apply_expected_state(committed_operations)
    expected_count = len(committed_operations)
    state_matches = recovered_snapshot["state"] == expected_state
    count_matches = recovered_snapshot["committed_operations"] == expected_count
    sequence_matches = recovered_snapshot["sequence"] == expected_count
    acknowledged_commits_recovered = state_matches and count_matches and sequence_matches

    try:
        import resource
        max_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except ImportError:
        max_rss_mb = None

    latency_summary = {
        "p50": round(percentile(latency_us, .50), 2),
        "p90": round(percentile(latency_us, .90), 2),
        "p99": round(percentile(latency_us, .99), 2),
        "p99_9": round(percentile(latency_us, .999), 2),
        "mean": round(statistics.mean(latency_us), 2) if latency_us else 0,
    }
    result = {
        "schema_version": "1.0",
        "operations_offered": len(offered_operations),
        "operations_delivered_to_engine": len(delivered_operations),
        "operations_committed_acknowledged": expected_count,
        "operations_failed_delivery": delivery_failures,
        "operations_failed_processing": processing_failures,
        "transport_attempts_total": total_attempts,
        "simulated_drops_total": total_drops,
        "retry_attempts_total": max(0, total_attempts - len(offered_operations)),
        "processing_errors": processing_errors,
        "unhandled_worker_exceptions": worker_exceptions,
        "elapsed_seconds": round(elapsed, 6),
        "offered_rate_ops_sec": round(len(offered_operations) / elapsed, 2) if elapsed else 0,
        "delivered_rate_ops_sec": round(len(delivered_operations) / elapsed, 2) if elapsed else 0,
        "acknowledged_commit_throughput_ops_sec": round(expected_count / elapsed, 2) if elapsed else 0,
        "target_offered_rate_ops_sec": args.target_rate,
        "workers": args.workers,
        "batch_size": args.batch_size,
        "configured_packet_drop_rate": args.drop_rate,
        "durability": {"wal_fsync_enabled": True, "recovery_reopened_wal": True},
        "latency_us": latency_summary,
        "resource_usage": {
            "process_cpu_seconds": round(cpu_seconds, 4),
            "max_resident_memory_mb": round(max_rss_mb, 2) if max_rss_mb is not None else None,
        },
        "live_snapshot": live_snapshot,
        "recovered_snapshot": recovered_snapshot,
        "recovery_integrity": {
            "acknowledged_commits_recovered": acknowledged_commits_recovered,
            "state_matches_expected": state_matches,
            "committed_count_matches": count_matches,
            "sequence_matches": sequence_matches,
        },
        "integrity_check": "PASS" if acknowledged_commits_recovered and not worker_exceptions else "FAIL",
        "wal_path": str(wal_path),
    }
    Path(args.output).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    recovered_engine.close()
    if result["integrity_check"] != "PASS":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
