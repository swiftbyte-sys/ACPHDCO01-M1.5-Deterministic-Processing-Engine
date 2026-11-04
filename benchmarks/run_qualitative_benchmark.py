"""Concurrent qualitative evidence-ingestion benchmark with lossy transport.

The failure injector drops requests before receiver processing. Retries reuse
the same event ID. This is an application-level transport simulation, not a
real network packet-loss experiment.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import json
import random
import statistics
import tempfile
import time

from acp_engine.qualitative import QualitativeResearchEngine


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--drop-rate", type=float, default=0.30)
    parser.add_argument("--max-attempts", type=int, default=20)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--wal", type=Path)
    parser.add_argument("--output", type=Path, default=Path("m3.5-qualitative-benchmark.json"))
    args = parser.parse_args()

    if args.ops < 1 or args.workers < 1 or not 0 <= args.drop_rate < 1:
        parser.error("ops/workers must be positive and drop-rate must be in [0, 1)")
    if args.max_attempts < 1:
        parser.error("max-attempts must be positive")

    temporary = args.wal is None
    if temporary:
        handle = tempfile.NamedTemporaryFile(prefix="m35-qualitative-", suffix=".wal", delete=False)
        wal_path = Path(handle.name)
        handle.close()
    else:
        wal_path = args.wal
        wal_path.parent.mkdir(parents=True, exist_ok=True)
        if wal_path.exists():
            wal_path.unlink()

    engine = QualitativeResearchEngine(wal_path, fsync=True)
    latencies_us: list[float] = []
    retry_attempts = 0
    failed = 0
    start = time.perf_counter()

    def deliver(index: int) -> tuple[float, int, bool]:
        rng = random.Random(args.seed + index)
        began = time.perf_counter_ns()
        event_id = f"evidence-event-{index}"
        for attempt in range(args.max_attempts):
            if rng.random() < args.drop_rate:
                # Decorrelated randomized delay, bounded to avoid retry storms.
                delay = min(0.01, 0.0001 * (2 ** min(attempt, 7)))
                time.sleep(rng.uniform(delay * 0.5, delay * 1.5))
                continue
            engine.add_evidence(
                event_id=event_id,
                evidence_id=f"evidence-{index}",
                source_id=f"source-{index % 100}",
                source_type=("interview" if index % 2 == 0 else "incident-report"),
                excerpt=f"Synthetic qualitative evidence excerpt {index}",
                metadata={"workload": "synthetic", "ordinal": index},
            )
            return (time.perf_counter_ns() - began) / 1000, attempt, True
        return (time.perf_counter_ns() - began) / 1000, args.max_attempts, False

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(deliver, index) for index in range(args.ops)]
        for future in as_completed(futures):
            latency, retries, succeeded = future.result()
            latencies_us.append(latency)
            retry_attempts += retries
            if not succeeded:
                failed += 1

    elapsed = time.perf_counter() - start
    snapshot = engine.snapshot()
    committed = snapshot["evidence_records"]
    result = {
        "operations_offered": args.ops,
        "operations_delivered": args.ops - failed,
        "operations_committed": committed,
        "operations_failed": failed,
        "workers": args.workers,
        "simulated_request_drop_rate": args.drop_rate,
        "retry_attempts_total": retry_attempts,
        "elapsed_seconds": round(elapsed, 6),
        "committed_throughput_ops_sec": round(committed / elapsed, 2) if elapsed else 0,
        "latency_us": {
            "p50": round(percentile(latencies_us, 0.50), 2),
            "p90": round(percentile(latencies_us, 0.90), 2),
            "p99": round(percentile(latencies_us, 0.99), 2),
            "p99_9": round(percentile(latencies_us, 0.999), 2),
            "mean": round(statistics.fmean(latencies_us), 2) if latencies_us else 0,
        },
        "durable_wal_fsync": True,
        "integrity_check": (
            "PASS" if committed == args.ops and failed == 0
            and snapshot["sequence"] == args.ops else "FAIL"
        ),
        "snapshot": snapshot,
        "wal_path": str(wal_path),
        "failure_model": "application-level request drop before receiver processing; bounded retries",
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    if temporary:
        wal_path.unlink(missing_ok=True)
    if result["integrity_check"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
