"""Thread-safe structured JSON telemetry for deterministic engine operations."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, TextIO
import json
import time
import uuid


class JsonTelemetry:
    """Append one machine-readable UTC event per completed operation.

    A single recorder serializes writes with a lock. The recorder does not
    swallow sink errors: telemetry failures remain visible to the caller.
    """

    def __init__(self, sink: str | Path | TextIO):
        self._sink = sink
        self._lock = Lock()

    @staticmethod
    def _utc_timestamp() -> str:
        return (
            datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    def record(
        self,
        *,
        operation: str,
        trace_id: str,
        started_ns: int,
        status_code: str,
        error_type: str | None = None,
        completed_ns: int | None = None,
    ) -> dict[str, Any]:
        if not operation or not isinstance(operation, str):
            raise ValueError("operation must be non-empty text")
        if not trace_id or not isinstance(trace_id, str):
            raise ValueError("trace_id must be non-empty text")
        if not isinstance(started_ns, int) or started_ns < 0:
            raise ValueError("started_ns must be a non-negative integer")
        if not status_code or not isinstance(status_code, str):
            raise ValueError("status_code must be non-empty text")

        end_ns = time.perf_counter_ns() if completed_ns is None else completed_ns
        if not isinstance(end_ns, int) or end_ns < started_ns:
            raise ValueError("completed_ns must be an integer greater than or equal to started_ns")

        event: dict[str, Any] = {
            "timestamp": self._utc_timestamp(),
            "trace_id": trace_id,
            "operation": operation,
            "latency_us": round((end_ns - started_ns) / 1000, 2),
            "status_code": status_code,
        }
        if error_type is not None:
            event["error_type"] = error_type

        encoded = json.dumps(
            event, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
            allow_nan=False,
        )
        with self._lock:
            if hasattr(self._sink, "write"):
                self._sink.write(encoded + "\n")
                if hasattr(self._sink, "flush"):
                    self._sink.flush()
            else:
                path = Path(self._sink)
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as stream:
                    stream.write(encoded + "\n")
        return event

    def execute(
        self,
        operation: str,
        function,
        *args,
        trace_id: str | None = None,
        **kwargs,
    ):
        """Execute a callable, log OK/ERROR, and preserve its return/exception."""
        started_ns = time.perf_counter_ns()
        correlation_id = trace_id or str(uuid.uuid4())
        try:
            result = function(*args, **kwargs)
        except Exception as exc:
            self.record(
                operation=operation,
                trace_id=correlation_id,
                started_ns=started_ns,
                status_code="ERROR",
                error_type=type(exc).__name__,
            )
            raise
        self.record(
            operation=operation,
            trace_id=correlation_id,
            started_ns=started_ns,
            status_code="OK",
        )
        return result
