# Assumptions and academic boundaries

1. The synthetic operation is a scalar state transition on a named key. `add` and `set` are deterministic operations.
2. Operation IDs are globally unique within the experiment and retained in the WAL.
3. Packet loss is modeled at the transport boundary before delivery. Lost acknowledgements are represented by replaying the same operation ID in a test.
4. The engine uses one process and one WAL file. Its lock provides process-local linearizability only.
5. Durability depends on filesystem and storage device guarantees. `fsync` is requested; this does not prove protection from every hardware/controller failure.
6. The concurrency design deliberately favors correctness and a clear commit point over lock-free throughput. A future multi-process version would require a transactional storage layer or consensus protocol.
7. The course incident's retry storm motivates decorrelated jitter, bounded retries, idempotency, admission control and observability. This minimal lab implements bounded retries and idempotency; rate limiting and multi-plane QoS are discussed as extensions rather than claimed as implemented.
8. No real network packets are dropped; the 30% failure is injected deterministically in the test transport simulator.
