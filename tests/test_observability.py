"""Tests for M4.5 structured telemetry."""
from datetime import datetime
from io import StringIO
import json
import time

import pytest

from acp_engine.observability import JsonTelemetry


def test_execute_emits_required_json_fields_and_returns_value():
    sink = StringIO()
    telemetry = JsonTelemetry(sink)
    result = telemetry.execute(
        "evidence_ingest", lambda value: value * 2, 21, trace_id="trace-001"
    )
    event = json.loads(sink.getvalue())
    assert result == 42
    assert event["trace_id"] == "trace-001"
    assert event["operation"] == "evidence_ingest"
    assert event["status_code"] == "OK"
    assert event["latency_us"] >= 0
    assert event["timestamp"].endswith("Z")
    datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))


def test_execute_logs_error_and_reraises_original_exception():
    sink = StringIO()
    telemetry = JsonTelemetry(sink)

    def fail():
        raise RuntimeError("expected failure")

    with pytest.raises(RuntimeError, match="expected failure"):
        telemetry.execute("failing_operation", fail, trace_id="trace-error")

    event = json.loads(sink.getvalue())
    assert event["status_code"] == "ERROR"
    assert event["error_type"] == "RuntimeError"
    assert event["trace_id"] == "trace-error"


def test_record_appends_one_json_line_per_call():
    sink = StringIO()
    telemetry = JsonTelemetry(sink)
    for index in range(3):
        telemetry.record(
            operation="benchmark", trace_id=f"trace-{index}",
            started_ns=time.perf_counter_ns(), status_code="OK",
        )
    rows = [json.loads(line) for line in sink.getvalue().splitlines()]
    assert len(rows) == 3
    assert len({row["trace_id"] for row in rows}) == 3


def test_file_sink_creates_parent_and_writes_json(tmp_path):
    path = tmp_path / "nested" / "events.jsonl"
    JsonTelemetry(path).execute("write", lambda: "done", trace_id="file-trace")
    event = json.loads(path.read_text(encoding="utf-8"))
    assert event["status_code"] == "OK"
    assert event["trace_id"] == "file-trace"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"operation": "", "trace_id": "t", "started_ns": 0, "status_code": "OK"},
        {"operation": "x", "trace_id": "", "started_ns": 0, "status_code": "OK"},
        {"operation": "x", "trace_id": "t", "started_ns": -1, "status_code": "OK"},
        {"operation": "x", "trace_id": "t", "started_ns": 0, "status_code": ""},
    ],
)
def test_record_rejects_invalid_fields(kwargs):
    with pytest.raises(ValueError):
        JsonTelemetry(StringIO()).record(**kwargs)


def test_record_uses_supplied_completion_timestamp_for_latency():
    sink = StringIO()
    event = JsonTelemetry(sink).record(
        operation="fixed_interval",
        trace_id="trace-fixed",
        started_ns=1_000_000,
        completed_ns=1_001_500,
        status_code="OK",
    )
    assert event["latency_us"] == 1.5
