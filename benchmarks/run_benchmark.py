#!/usr/bin/env python3
"""Concurrent throughput/latency and packet-loss benchmark."""
import argparse, concurrent.futures, json, logging, os, statistics, sys, tempfile, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from acp_engine import ProcessingEngine
from acp_engine.workload import make_operation
from acp_engine.transport import deliver_with_retries

def percentile(values, p):
    if not values: return 0.0
    xs = sorted(values)
    pos = (len(xs)-1) * p
    lo = int(pos); hi = min(lo+1, len(xs)-1)
    return xs[lo] + (xs[hi]-xs[lo])*(pos-lo)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ops", type=int, default=10000)
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--target-rate", type=float, default=10000)
    ap.add_argument("--drop-rate", type=float, default=0.30)
    ap.add_argument("--output", default="benchmark-results.json")
    ap.add_argument("--wal", default=None)
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING)
    wal = args.wal or tempfile.mktemp(prefix="acp-", suffix=".wal")
    engine = ProcessingEngine(wal, fsync=True)
    latencies=[]; attempts=[]; errors=0
    start=time.perf_counter()
    def task(i):
        op=make_operation(i)
        t=time.perf_counter_ns()
        try:
            res=deliver_with_retries(op, engine.process, drop_rate=args.drop_rate, seed=i)
            return (time.perf_counter_ns()-t)/1000, res["delivery_attempts"], None
        except Exception as e:
            return (time.perf_counter_ns()-t)/1000, 0, repr(e)
    # Open-loop pacing avoids an unrealistic unlimited burst at t=0.
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures=[]
        for i in range(args.ops):
            due=start + i / max(args.target_rate, 1)
            wait=due-time.perf_counter()
            if wait > 0: time.sleep(wait)
            futures.append(pool.submit(task, i))
        for f in concurrent.futures.as_completed(futures):
            lat, att, err=f.result(); latencies.append(lat); attempts.append(att)
            if err: errors += 1
    elapsed=time.perf_counter()-start
    snap=engine.snapshot()
    result={"operations_submitted":args.ops,"operations_committed":snap["committed_operations"],
      "errors":errors,"elapsed_seconds":round(elapsed,6),
      "throughput_ops_sec":round(snap["committed_operations"]/elapsed,2),
      "target_rate_ops_sec":args.target_rate,"workers":args.workers,"packet_drop_rate":args.drop_rate,
      "latency_us":{"p50":round(percentile(latencies,.50),2),"p90":round(percentile(latencies,.90),2),
        "p99":round(percentile(latencies,.99),2),"p99_9":round(percentile(latencies,.999),2),
        "mean":round(statistics.mean(latencies),2) if latencies else 0},
      "retry_attempts_mean":round(statistics.mean([x for x in attempts if x]),3) if any(attempts) else 0,
      "state_keys":len(snap["state"]),"wal_path":wal,
      "integrity_check":"PASS" if snap["committed_operations"]==args.ops and errors==0 else "FAIL"}
    Path(args.output).write_text(json.dumps(result,indent=2))
    print(json.dumps(result,indent=2))
    engine.close()
if __name__=="__main__": main()
