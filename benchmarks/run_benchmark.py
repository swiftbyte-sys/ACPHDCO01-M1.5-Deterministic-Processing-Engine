#!/usr/bin/env python3
"""Durable group-commit throughput, latency, and packet-loss benchmark."""
import argparse
import concurrent.futures
import json
import logging
import resource
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
    pos = (len(xs) - 1) * p
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ops", type=int, default=10000)
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--target-rate", type=float, default=10000)
    ap.add_argument("--drop-rate", type=float, default=0.30)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument("--output", default="benchmark-results.json")
    ap.add_argument("--wal", default=None)
    args = ap.parse_args()
    if min(args.ops, args.workers, args.batch_size) <= 0 or args.target_rate <= 0:
        ap.error("--ops, --workers, --batch-size, and --target-rate must be positive")
    if not 0 <= args.drop_rate < 1:
        ap.error("--drop-rate must be in [0, 1)")

    logging.basicConfig(level=logging.WARNING)
    wal = args.wal or tempfile.mktemp(prefix="acp-", suffix=".wal")
    engine = ProcessingEngine(wal, fsync=True)
    latencies = []
    attempts = []
    errors = 0
    cpu_start = time.process_time()
    start = time.perf_counter()

    def task(batch):
        accepted = []
        task_results = []
        starts = []
        for index, op in batch:
            op_start = time.perf_counter_ns()
            starts.append(op_start)
            try:
                delivered = deliver_with_retries(
                    op, lambda item: item, drop_rate=args.drop_rate,
                    seed=index,
                )
                accepted.append(delivered)
                task_results.append((index, None, delivered["delivery_attempts"]))
            except Exception as exc:
                task_results.append((index, repr(exc), 0))

        if accepted:
            engine.process_many(accepted)
        completed_ns = time.perf_counter_ns()
        return [
            ((completed_ns - op_start) / 1000, attempt_count, error)
            for op_start, (_, error, attempt_count) in zip(starts, task_results)
        ]

    # Open-loop pacing schedules batches against the offered operation rate.
    batches = []
    for offset in range(0, args.ops, args.batch_size):
        batch = [
            (i, make_operation(i))
            for i in range(offset, min(offset + args.batch_size, args.ops))
        ]
        batches.append((offset, batch))

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = []
        for offset, batch in batches:
            due = start + offset / args.target_rate
            wait = due - time.perf_counter()
            if wait > 0:
                time.sleep(wait)
            futures.append(pool.submit(task, batch))
        for future in concurrent.futures.as_completed(futures):
            for latency, attempt_count, error in future.result():
                latencies.append(latency)
                attempts.append(attempt_count)
                if error:
                    errors += 1

    elapsed = time.perf_counter() - start
    cpu_seconds = time.process_time() - cpu_start
    snap = engine.snapshot()
    delivered = args.ops - errors
    attempts_total = sum(attempts)
    max_rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    result = {
        "operations_offered": args.ops,
        "operations_delivered": delivered,
        "operations_committed": snap["committed_operations"],
        "operations_failed": errors,
        "transport_attempts_total": attempts_total,
        "retry_attempts_total": max(0, attempts_total - delivered),
        "elapsed_seconds": round(elapsed, 6),
        "offered_rate_ops_sec": round(args.ops / elapsed, 2),
        "delivered_rate_ops_sec": round(delivered / elapsed, 2),
        "committed_throughput_ops_sec": round(snap["committed_operations"] / elapsed, 2),
        "target_rate_ops_sec": args.target_rate,
        "workers": args.workers,
        "batch_size": args.batch_size,
        "packet_drop_rate": args.drop_rate,
        "durable_wal_fsync": True,
        "latency_us": {
            "p50": round(percentile(latencies, .50), 2),
            "p90": round(percentile(latencies, .90), 2),
            "p99": round(percentile(latencies, .99), 2),
            "p99_9": round(percentile(latencies, .999), 2),
            "mean": round(statistics.mean(latencies), 2) if latencies else 0,
        },
        "retry_attempts_mean": round(statistics.mean([x for x in attempts if x]), 3) if any(attempts) else 0,
        "resource_usage": {
            "process_cpu_seconds": round(cpu_seconds, 4),
            "max_resident_memory_mb": round(max_rss_mb, 2),
        },
        "state_keys": len(snap["state"]),
        "wal_path": wal,
        "integrity_check": "PASS" if snap["committed_operations"] == args.ops and errors == 0 else "FAIL",
        "integrity_invariants": {
            "all_offered_committed": snap["committed_operations"] == args.ops,
            "no_delivery_errors": errors == 0,
            "unique_commit_count_matches": snap["sequence"] == args.ops,
        },
    }
    Path(args.output).write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    engine.close()


if __name__ == "__main__":
    main()
