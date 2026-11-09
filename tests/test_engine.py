import json
import math
import threading

import pytest

from acp_engine import ProcessingEngine, ProcessingError
from acp_engine.transport import deliver_with_retries


def test_add_set_and_duplicate(tmp_path):
    e = ProcessingEngine(tmp_path / "x.wal")
    a = {"op_id": "1", "key": "balance", "action": "add", "value": 5}
    assert e.process(a)["value"] == 5
    assert e.process(a)["duplicate"] is True
    assert e.process({"op_id": "2", "key": "balance", "action": "set", "value": 9})["value"] == 9
    assert e.snapshot()["committed_operations"] == 2
    e.close()


def test_validation(tmp_path):
    e = ProcessingEngine(tmp_path / "x.wal")
    with pytest.raises(ProcessingError):
        e.process({"op_id": "1", "key": "x", "action": "delete", "value": 1})
    e.close()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True])
def test_rejects_non_finite_and_boolean_values(tmp_path, value):
    e = ProcessingEngine(tmp_path / "x.wal")
    with pytest.raises(ProcessingError):
        e.process({"op_id": "bad", "key": "x", "action": "set", "value": value})
    assert e.snapshot()["committed_operations"] == 0
    e.close()


def test_recovery(tmp_path):
    p = tmp_path / "x.wal"
    e = ProcessingEngine(p)
    e.process({"op_id": "1", "key": "x", "action": "add", "value": 4})
    e.process({"op_id": "2", "key": "x", "action": "add", "value": 3})
    e.close()

    e2 = ProcessingEngine(p)
    assert e2.snapshot()["state"]["x"] == 7
    assert e2.snapshot()["committed_operations"] == 2
    e2.close()


def test_concurrent_unique_operations(tmp_path):
    e = ProcessingEngine(tmp_path / "x.wal")

    def run(i):
        e.process({"op_id": str(i), "key": "counter", "action": "add", "value": 1})

    threads = [threading.Thread(target=run, args=(i,)) for i in range(300)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert e.snapshot()["state"]["counter"] == 300
    assert e.snapshot()["committed_operations"] == 300
    assert e.snapshot()["sequence"] == 300
    e.close()


def test_retry_30_percent_and_idempotency(tmp_path):
    e = ProcessingEngine(tmp_path / "x.wal")
    op = {"op_id": "stable", "key": "x", "action": "add", "value": 1}
    result = deliver_with_retries(op, e.process, drop_rate=0.30, seed=17)
    assert result["value"] == 1
    again = e.process(op)
    assert again["duplicate"] is True
    assert e.snapshot()["state"]["x"] == 1
    e.close()


def test_same_id_with_different_payload_is_rejected(tmp_path):
    e = ProcessingEngine(tmp_path / "x.wal")
    e.process({"op_id": "same", "key": "x", "action": "add", "value": 1})
    with pytest.raises(ProcessingError, match="different operation payload"):
        e.process({"op_id": "same", "key": "x", "action": "add", "value": 99})
    assert e.snapshot()["state"]["x"] == 1
    assert e.snapshot()["committed_operations"] == 1
    e.close()


def test_incomplete_tail_is_truncated_before_next_append(tmp_path):
    p = tmp_path / "x.wal"
    e = ProcessingEngine(p)
    e.process({"op_id": "1", "key": "x", "action": "add", "value": 2})
    e.close()

    with p.open("ab") as f:
        f.write(b'{"partial":')

    recovered = ProcessingEngine(p)
    assert recovered.snapshot()["state"]["x"] == 2
    assert p.read_bytes().endswith(b"\n")
    recovered.process({"op_id": "2", "key": "x", "action": "add", "value": 3})
    recovered.close()

    final = ProcessingEngine(p)
    assert final.snapshot()["state"]["x"] == 5
    assert final.snapshot()["committed_operations"] == 2
    final.close()


def test_complete_corrupt_wal_record_fails_closed(tmp_path):
    p = tmp_path / "bad.wal"
    p.write_bytes(b'{"not":"a valid operation record"}\n')
    with pytest.raises(ProcessingError, match="WAL recovery failed"):
        ProcessingEngine(p)


def test_idempotency_survives_restart(tmp_path):
    p = tmp_path / "restart.wal"
    first = ProcessingEngine(p)
    op = {"op_id": "persistent", "key": "x", "action": "add", "value": 7}
    first.process(op)
    first.close()

    second = ProcessingEngine(p)
    duplicate = second.process(op)
    assert duplicate["duplicate"] is True
    assert second.snapshot()["state"]["x"] == 7
    second.close()
