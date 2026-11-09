"""Thread-safe deterministic state machine with crash-safe WAL recovery.

The engine provides process-local linearizable commits. Every committed operation
is appended to the WAL before state mutation. A torn final WAL record is removed
during recovery; corruption in a complete record fails closed.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import math
import os
from pathlib import Path
import time
from threading import RLock
from typing import Any
import uuid

logger = logging.getLogger("acp.engine")


class ProcessingError(Exception):
    """Invalid, conflicting, or unprocessable operation."""


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
            if isinstance(raw.get("value"), bool):
                raise ValueError("value must be numeric, not boolean")
            value = float(raw["value"])
            op = cls(
                op_id=str(raw["op_id"]),
                key=str(raw["key"]),
                action=str(raw["action"]),
                value=value,
                trace_id=str(raw.get("trace_id") or uuid.uuid4().hex),
            )
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ProcessingError(f"invalid operation fields: {exc}") from exc

        if not op.op_id or len(op.op_id) > 128:
            raise ProcessingError("op_id must contain 1..128 characters")
        if not op.key or len(op.key) > 128:
            raise ProcessingError("key must contain 1..128 characters")
        if op.action not in {"add", "set"}:
            raise ProcessingError("action must be 'add' or 'set'")
        if not math.isfinite(op.value) or not (-1e15 <= op.value <= 1e15):
            raise ProcessingError("value must be finite and within supported bounds")
        if not op.trace_id or len(op.trace_id) > 128:
            raise ProcessingError("trace_id must contain 1..128 characters")
        return op

    def fingerprint(self) -> tuple[str, str, float]:
        # trace_id is deliberately excluded: a legitimate retry may have a new trace.
        return (self.key, self.action, self.value)


class ProcessingEngine:
    """Linearizable in-process state machine with an append-only durable WAL."""

    def __init__(self, wal_path: str | Path, *, fsync: bool = True):
        self.wal_path = Path(wal_path)
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self._lock = RLock()
        self._state: dict[str, float] = {}
        self._results: dict[str, dict[str, Any]] = {}
        self._fingerprints: dict[str, tuple[str, str, float]] = {}
        self._sequence = 0
        self._closed = False
        self._recover()
        # Keep one unbuffered append handle open; avoid open/close on every commit.
        self._wal = self.wal_path.open("ab", buffering=0)

    @staticmethod
    def _utc_now() -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    def _emit(
        self,
        op: Operation,
        started_ns: int,
        status: str,
        sequence: int | None = None,
    ) -> None:
        record = {
            "timestamp": self._utc_now(),
            "trace_id": op.trace_id,
            "op_id": op.op_id,
            "latency_us": round((time.perf_counter_ns() - started_ns) / 1000, 3),
            "status_code": status,
            "sequence": sequence,
        }
        logger.info(json.dumps(record, separators=(",", ":")))

    def _recover(self) -> None:
        if not self.wal_path.exists():
            self.wal_path.touch()
            return

        valid_end = 0
        with self.wal_path.open("r+b") as fh:
            line_number = 0
            while True:
                line = fh.readline()
                if not line:
                    break
                line_number += 1
                if not line.endswith(b"\n"):
                    # Only a torn final append is recoverable. Remove it so future
                    # appends cannot turn the partial bytes into a corrupt record.
                    fh.seek(valid_end)
                    fh.truncate()
                    fh.flush()
                    if self.fsync:
                        os.fsync(fh.fileno())
                    break
                try:
                    rec = json.loads(line)
                    op = Operation.from_dict(rec["operation"])
                    seq = int(rec["sequence"])
                    if seq != self._sequence + 1:
                        raise ProcessingError(
                            f"non-contiguous WAL sequence at line {line_number}"
                        )
                    if op.op_id in self._fingerprints:
                        raise ProcessingError(f"duplicate WAL operation ID at line {line_number}")
                    self._apply(op)
                    result = {
                        "op_id": op.op_id,
                        "key": op.key,
                        "value": self._state[op.key],
                        "sequence": seq,
                        "recovered": True,
                    }
                    self._results[op.op_id] = result
                    self._fingerprints[op.op_id] = op.fingerprint()
                    self._sequence = seq
                    valid_end = fh.tell()
                except Exception as exc:
                    raise ProcessingError(
                        f"WAL recovery failed at line {line_number}: {exc}"
                    ) from exc

    def _apply(self, op: Operation) -> None:
        old = self._state.get(op.key, 0.0)
        self._state[op.key] = op.value if op.action == "set" else old + op.value

    def process(self, raw: dict[str, Any]) -> dict[str, Any]:
        started = time.perf_counter_ns()
        op = Operation.from_dict(raw)
        with self._lock:
            if self._closed:
                raise ProcessingError("engine is closed")

            existing = self._results.get(op.op_id)
            if existing is not None:
                if self._fingerprints[op.op_id] != op.fingerprint():
                    self._emit(op, started, "409_IDEMPOTENCY_CONFLICT", existing["sequence"])
                    raise ProcessingError(
                        "op_id was already committed with a different operation payload"
                    )
                result = dict(existing)
                result["duplicate"] = True
                self._emit(op, started, "200_DUPLICATE", result["sequence"])
                return result

            seq = self._sequence + 1
            record = {
                "sequence": seq,
                "operation": {
                    "op_id": op.op_id,
                    "key": op.key,
                    "action": op.action,
                    "value": op.value,
                    "trace_id": op.trace_id,
                },
            }
            encoded = (
                json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)
                + "\n"
            ).encode("utf-8")

            # Write-ahead rule: durable record first, then visible state mutation.
            written = self._wal.write(encoded)
            if written != len(encoded):
                raise OSError(f"short WAL write: {written} of {len(encoded)} bytes")
            if self.fsync:
                os.fsync(self._wal.fileno())

            self._apply(op)
            self._sequence = seq
            result = {
                "op_id": op.op_id,
                "key": op.key,
                "value": self._state[op.key],
                "sequence": seq,
                "duplicate": False,
            }
            self._results[op.op_id] = result
            self._fingerprints[op.op_id] = op.fingerprint()
            self._emit(op, started, "200_OK", seq)
            return result

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "sequence": self._sequence,
                "state": dict(self._state),
                "committed_operations": len(self._results),
            }

    def get_result(self, op_id: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._results.get(op_id)
            return dict(value) if value else None

    def close(self) -> None:
        """Flush and close the WAL; safe to call more than once."""
        with self._lock:
            if self._closed:
                return
            self._wal.flush()
            if self.fsync:
                os.fsync(self._wal.fileno())
            self._wal.close()
            self._closed = True

    def __enter__(self) -> "ProcessingEngine":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
