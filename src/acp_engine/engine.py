"""Thread-safe deterministic state machine with write-ahead logging.

The engine serializes commits under one lock. Worker threads may call process()
concurrently; the lock provides a linearizable commit order. WAL records are
fsync'd before in-memory state is changed (write-ahead rule).
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any
import json, logging, time, uuid
from datetime import datetime, timezone

logger = logging.getLogger("acp.engine")

class ProcessingError(Exception):
    """Invalid or unprocessable operation."""

@dataclass(frozen=True)
class Operation:
    op_id: str
    key: str
    action: str
    value: float
    trace_id: str = ""

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Operation":
        if not isinstance(raw, dict):
            raise ProcessingError("operation must be a JSON object")
        try:
            op = cls(
                op_id=str(raw["op_id"]), key=str(raw["key"]),
                action=str(raw["action"]), value=float(raw["value"]),
                trace_id=str(raw.get("trace_id") or uuid.uuid4().hex)
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ProcessingError(f"invalid operation fields: {exc}") from exc
        if not op.op_id or len(op.op_id) > 128:
            raise ProcessingError("op_id must contain 1..128 characters")
        if not op.key or len(op.key) > 128:
            raise ProcessingError("key must contain 1..128 characters")
        if op.action not in {"add", "set"}:
            raise ProcessingError("action must be 'add' or 'set'")
        if not (-1e15 <= op.value <= 1e15):
            raise ProcessingError("value outside supported numeric bounds")
        return op

class ProcessingEngine:
    """Linearizable in-process state machine with durable append-only WAL."""
    def __init__(self, wal_path: str | Path, *, fsync: bool = True):
        self.wal_path = Path(wal_path)
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self._lock = RLock()
        self._state: dict[str, float] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._sequence = 0
        self._recover()

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _emit(self, op: Operation, started_ns: int, status: str, sequence: int | None = None):
        if not logger.isEnabledFor(logging.INFO):
            return
        record = {
            "timestamp": self._utc_now(),
            "trace_id": op.trace_id,
            "op_id": op.op_id,
            "latency_us": round((time.perf_counter_ns() - started_ns) / 1000, 3),
            "status_code": status,
            "sequence": sequence,
        }
        logger.info(json.dumps(record, separators=(",", ":")))

    def _recover(self):
        if not self.wal_path.exists():
            self.wal_path.touch()
            return
        # A final incomplete line is ignored; all complete records are validated.
        with self.wal_path.open("rb") as fh:
            lines = fh.readlines()
        for idx, line in enumerate(lines):
            if not line.endswith(b"\n"):
                if idx == len(lines) - 1:
                    break
                raise ProcessingError(f"corrupt WAL record at line {idx+1}")
            try:
                rec = json.loads(line)
                op = Operation.from_dict(rec["operation"])
                seq = int(rec["sequence"])
                if seq != self._sequence + 1:
                    raise ProcessingError(f"non-contiguous WAL sequence at line {idx+1}")
                self._apply(op)
                result = {"op_id": op.op_id, "key": op.key, "value": self._state[op.key],
                          "sequence": seq, "recovered": True}
                self._results[op.op_id] = result
                self._sequence = seq
            except Exception as exc:
                raise ProcessingError(f"WAL recovery failed at line {idx+1}: {exc}") from exc

    def _apply(self, op: Operation):
        old = self._state.get(op.key, 0.0)
        self._state[op.key] = op.value if op.action == "set" else old + op.value

    def process(self, raw: dict[str, Any]) -> dict[str, Any]:
        started = time.perf_counter_ns()
        op = Operation.from_dict(raw)
        with self._lock:
            existing = self._results.get(op.op_id)
            if existing is not None:
                result = dict(existing)
                result["duplicate"] = True
                self._emit(op, started, "200_DUPLICATE", result["sequence"])
                return result
            seq = self._sequence + 1
            record = {"sequence": seq, "operation": {
                "op_id": op.op_id, "key": op.key, "action": op.action,
                "value": op.value, "trace_id": op.trace_id}}
            encoded = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
            # WAL first, then state: a crash after WAL write is recovered on restart.
            with self.wal_path.open("ab", buffering=0) as fh:
                fh.write(encoded)
                if self.fsync:
                    import os
                    os.fsync(fh.fileno())
            self._apply(op)
            self._sequence = seq
            result = {"op_id": op.op_id, "key": op.key, "value": self._state[op.key],
                      "sequence": seq, "duplicate": False}
            self._results[op.op_id] = result
            self._emit(op, started, "200_OK", seq)
            return result

    def process_many(self, raws: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Commit a batch with one WAL fsync (group commit).

        The batch is serialized under the same lock as process(). A complete
        WAL prefix is durable before any corresponding in-memory state is
        published. Repeated operation IDs within or before the batch remain
        idempotent.
        """
        if not isinstance(raws, list):
            raise ProcessingError("raws must be a list")
        if not raws:
            return []
        started = [time.perf_counter_ns() for _ in raws]
        operations = [Operation.from_dict(raw) for raw in raws]

        with self._lock:
            working_state = dict(self._state)
            next_sequence = self._sequence
            pending: dict[str, dict[str, Any]] = {}
            records = []
            output_specs = []

            for op in operations:
                existing = self._results.get(op.op_id) or pending.get(op.op_id)
                if existing is not None:
                    output_specs.append((op, dict(existing), True))
                    continue

                next_sequence += 1
                old = working_state.get(op.key, 0.0)
                value = op.value if op.action == "set" else old + op.value
                working_state[op.key] = value
                result = {
                    "op_id": op.op_id, "key": op.key, "value": value,
                    "sequence": next_sequence, "duplicate": False,
                }
                pending[op.op_id] = result
                records.append({
                    "sequence": next_sequence,
                    "operation": {
                        "op_id": op.op_id, "key": op.key, "action": op.action,
                        "value": op.value, "trace_id": op.trace_id,
                    },
                })
                output_specs.append((op, dict(result), False))

            if records:
                payload = b"".join(
                    (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
                    for record in records
                )
                with self.wal_path.open("ab", buffering=0) as fh:
                    view = memoryview(payload)
                    while view:
                        written = fh.write(view)
                        if not written:
                            raise OSError("short write while appending WAL batch")
                        view = view[written:]
                    if self.fsync:
                        import os
                        os.fsync(fh.fileno())

                self._state = working_state
                self._sequence = next_sequence
                self._results.update(pending)

            results = []
            for index, (op, result, duplicate) in enumerate(output_specs):
                if duplicate:
                    result["duplicate"] = True
                self._emit(
                    op, started[index],
                    "200_DUPLICATE" if duplicate else "200_OK",
                    result["sequence"],
                )
                results.append(result)
            return results

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {"sequence": self._sequence, "state": dict(self._state),
                    "committed_operations": len(self._results)}

    def get_result(self, op_id: str):
        with self._lock:
            value = self._results.get(op_id)
            return dict(value) if value else None

    def close(self):
        """Reserved lifecycle hook for interchangeable storage backends."""
        return None
