import json
import logging

import pytest

from acp_engine import ProcessingEngine, ProcessingError


def op(op_id, *, key="counter", action="add", value=1, trace_id="trace-1"):
    return {
        "op_id": op_id,
        "key": key,
        "action": action,
        "value": value,
        "trace_id": trace_id,
    }


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True])
def test_rejects_non_finite_or_boolean_values(tmp_path, value):
    engine = ProcessingEngine(tmp_path / "validation.wal")
    with pytest.raises(ProcessingError):
        engine.process(op("invalid", value=value))


def test_rejects_reuse_of_operation_id_with_different_payload(tmp_path):
    engine = ProcessingEngine(tmp_path / "idempotency.wal")
    engine.process(op("stable", value=2))
    with pytest.raises(ProcessingError, match="different operation"):
        engine.process(op("stable", value=99))
    assert engine.snapshot()["state"]["counter"] == 2
    assert engine.snapshot()["committed_operations"] == 1


def test_batch_rejects_conflicting_duplicate_operation_id(tmp_path):
    engine = ProcessingEngine(tmp_path / "batch-conflict.wal")
    with pytest.raises(ProcessingError, match="different operation"):
        engine.process_many([op("same", value=1), op("same", value=5)])
    assert engine.snapshot()["committed_operations"] == 0
    assert engine.snapshot()["sequence"] == 0


def test_torn_wal_tail_is_truncated_before_future_append(tmp_path):
    wal = tmp_path / "torn.wal"
    engine = ProcessingEngine(wal)
    engine.process(op("first", value=2))
    with wal.open("ab") as stream:
        stream.write(b'{"incomplete":')

    recovered = ProcessingEngine(wal)
    assert recovered.snapshot()["state"]["counter"] == 2
    recovered.process(op("second", value=3))

    recovered_again = ProcessingEngine(wal)
    assert recovered_again.snapshot()["state"]["counter"] == 5
    assert recovered_again.snapshot()["sequence"] == 2
    assert recovered_again.snapshot()["committed_operations"] == 2


def test_idempotency_conflict_is_preserved_after_recovery(tmp_path):
    wal = tmp_path / "recovered-idempotency.wal"
    engine = ProcessingEngine(wal)
    engine.process(op("persisted", value=4))

    recovered = ProcessingEngine(wal)
    with pytest.raises(ProcessingError, match="different operation"):
        recovered.process(op("persisted", value=400))
    assert recovered.snapshot()["state"]["counter"] == 4


def test_telemetry_is_structured_json_with_required_fields(tmp_path, caplog):
    engine = ProcessingEngine(tmp_path / "telemetry.wal")
    with caplog.at_level(logging.INFO, logger="acp.engine"):
        engine.process(op("telemetry-op", trace_id="trace-visible"))

    records = [json.loads(record.message) for record in caplog.records
               if record.name == "acp.engine"]
    assert records
    event = records[-1]
    assert event["trace_id"] == "trace-visible"
    assert event["op_id"] == "telemetry-op"
    assert event["status_code"] == "200_OK"
    assert isinstance(event["latency_us"], (int, float))
    assert event["latency_us"] >= 0
    assert event["timestamp"].endswith("Z")
    assert event["sequence"] == 1
