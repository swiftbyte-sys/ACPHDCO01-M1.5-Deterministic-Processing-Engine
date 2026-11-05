"""Paced M4.5 concurrent benchmark with bounded application-level loss injection."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json
import os
import platform
import random
import statistics
import tempfile
import time

from acp_engine.observability import JsonTelemetry
from acp_engine.qualitative import QualitativeResearchEngine


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ops", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--target-rate", type=int, default=10000,
                        help="offered operations per second")
    parser.add_argument("--drop-rate", type=float, default=0.30)
    parser.add_argument("--max-attempts", type=int, default=20)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--wal", type=Path)
    parser.add_argument("--telemetry", type=Path, default=Path("m4.5-telemetry.jsonl"))
    parser.add_argument("--output", type=Path, default=Path("m4.5-benchmark.json"))
    args = parser.parse_args()

    if args.ops < 1 or args.workers < 1 or args.target_rate < 1:
        parser.error("ops, workers, and target-rate must be positive")
    if not 0 <= args.drop_rate < 1:
        parser.error("drop-rate must be in [0, 1)")
    if args.max_attempts < 1:
        parser.error("max-attempts must be positive")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.telemetry.parent.mkdir(parents=True, exist_ok=True)
    temp_dir = tempfile.TemporaryDirectory(prefix="m45-benchmark-") if args.wal is None else None
    wal_path = args.wal or (Path(temp_dir.name) / "engine.wal")
    wal_path.parent.mkdir(parents=True, exist_ok=True)
    if args.wal is not None and wal_path.exists():
        parser.error(f"WAL already exists; choose a fresh path: {wal_path}")
    if args.telemetry.exists():
        args.telemetry.unlink()
    if args.output.exists():
        args.output.unlink()

    engine = QualitativeResearchEngine(wal_path, fsync=True)
    telemetry = JsonTelemetry(args.telemetry)
    latencies_us: list[float] = []
    retries_total = 0
    drops_total = 0
    failures = 0
    start_ns = time.perf_counter_ns()

    def deliver(index: int, submitted_ns: int) -> dict:
        rng = random.Random(args.seed + index)
        event_id = f"m4-5-event-{index}"
        retries = 0
        for attempt in range(args.max_attempts):
            if rng.random() < args.drop_rate:
                retries += 1
                drops = min(0.01, 0.0001 * (2 ** min(attempt, 7)))
                time.sleep(rng.uniform(drops * 0.5, drops * 1.5))
                continue
            engine.add_evidence(
                event_id=event_id,
                evidence_id=f"m4-5-evidence-{index}",
                source_id=f"synthetic-source-{index % 100}",
                source_type="synthetic-benchmark",
                excerpt=f"Synthetic evidence operation {index}",
                metadata={"lab": "ACPHDCO01-M4.5", "ordinal": index},
            )
            return {
                "index": index,
                "submitted_ns": submitted_ns,
                "completed_ns": time.perf_counter_ns(),
                "retries": retries,
                "status": "COMMITTED",
            }
        return {
            "index": index,
            "submitted_ns": submitted_ns,
            "completed_ns": time.perf_counter_ns(),
            "retries": retries,
            "status": "RETRY_EXHAUSTED",
        }

    results = []
    futures = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for index in range(args.ops):
            target_ns = start_ns + int(index * 1_000_000_000 / args.target_rate)
            remaining_ns = target_ns - time.perf_counter_ns()
            if remaining_ns > 0:
                time.sleep(remaining_ns / 1_000_000_000)
            submitted_ns = time.perf_counter_ns()
            future = pool.submit(deliver, index, submitted_ns)
            futures[future] = (index, submitted_ns)

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
                latency = (result["completed_ns"] - result["submitted_ns"]) / 1000
                latencies_us.append(latency)
                retries_total += result["retries"]
                drops_total += result["retries"]
                status = result["status"]
                if status != "COMMITTED":
                    failures += 1
                telemetry.record(
                    operation="evidence_ingest_with_retry",
                    trace_id=f"m4.5-{result['index']}",
                    started_ns=result["submitted_ns"],
                    status_code=status,
                    completed_ns=result["completed_ns"],
                )
            except Exception as exc:
                failures += 1
                index, submitted_ns = futures[future]
                completed_ns = time.perf_counter_ns()
                latencies_us.append((completed_ns - submitted_ns) / 1000)
                telemetry.record(
                    operation="evidence_ingest_with_retry",
                    trace_id=f"m4.5-{index}",
                    started_ns=submitted_ns,
                    status_code="ERROR",
                    error_type=type(exc).__name__,
                    completed_ns=completed_ns,
                )

    finish_ns = time.perf_counter_ns()
    elapsed = (finish_ns - start_ns) / 1_000_000_000
    snapshot = engine.snapshot()
    committed = snapshot["evidence_records"]
    successful = sum(1 for item in results if item["status"] == "COMMITTED")
    integrity = (
        "PASS" if committed == args.ops and successful == args.ops
        and failures == 0 and snapshot["sequence"] == args.ops else "FAIL"
    )
    result = {
        "lab": "ACPHDCO01-M4.5",
        "operations_offered": args.ops,
        "operations_delivered": successful,
        "operations_committed": committed,
        "operations_failed": failures,
        "workers": args.workers,
        "target_offered_rate_ops_sec": args.target_rate,
        "observed_offered_rate_ops_sec": round(args.ops / elapsed, 2) if elapsed else 0,
        "committed_throughput_ops_sec": round(committed / elapsed, 2) if elapsed else 0,
        "simulated_application_request_drop_rate": args.drop_rate,
        "simulated_drops": drops_total,
        "retry_attempts_total": retries_total,
        "max_attempts_per_operation": args.max_attempts,
        "elapsed_seconds": round(elapsed, 6),
        "latency_us": {
            "p50": round(percentile(latencies_us, 0.50), 2),
            "p90": round(percentile(latencies_us, 0.90), 2),
            "p99": round(percentile(latencies_us, 0.99), 2),
            "p99_9": round(percentile(latencies_us, 0.999), 2),
            "mean": round(statistics.fmean(latencies_us), 2) if latencies_us else 0,
        },
        "durable_wal_fsync": True,
        "integrity_check": integrity,
        "snapshot": snapshot,
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count_logical": os.cpu_count(),
        },
        "artifacts": {
            "wal_path": str(wal_path),
            "telemetry_path": str(args.telemetry),
        },
        "failure_model": (
            "Deterministic-seeded application-level request drops before receiver "
            "processing; bounded randomized retry delays; stable event IDs. "
            "This is not physical network packet loss."
        ),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if temp_dir is not None:
        temp_dir.cleanup()
    if integrity != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
