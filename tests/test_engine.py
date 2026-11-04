import json, threading
import pytest
from acp_engine import ProcessingEngine, ProcessingError
from acp_engine.transport import deliver_with_retries, DeliveryError

def test_add_set_and_duplicate(tmp_path):
    e=ProcessingEngine(tmp_path/"x.wal")
    a={"op_id":"1","key":"balance","action":"add","value":5}
    assert e.process(a)["value"]==5
    assert e.process(a)["duplicate"] is True
    assert e.process({"op_id":"2","key":"balance","action":"set","value":9})["value"]==9
    assert e.snapshot()["committed_operations"]==2

def test_validation(tmp_path):
    e=ProcessingEngine(tmp_path/"x.wal")
    with pytest.raises(ProcessingError):
        e.process({"op_id":"1","key":"x","action":"delete","value":1})

def test_recovery(tmp_path):
    p=tmp_path/"x.wal"
    e=ProcessingEngine(p)
    e.process({"op_id":"1","key":"x","action":"add","value":4})
    e.process({"op_id":"2","key":"x","action":"add","value":3})
    e2=ProcessingEngine(p)
    assert e2.snapshot()["state"]["x"]==7
    assert e2.snapshot()["committed_operations"]==2

def test_concurrent_unique_operations(tmp_path):
    e=ProcessingEngine(tmp_path/"x.wal")
    def run(i):
        e.process({"op_id":str(i),"key":"counter","action":"add","value":1})
    threads=[threading.Thread(target=run,args=(i,)) for i in range(300)]
    [t.start() for t in threads]; [t.join() for t in threads]
    assert e.snapshot()["state"]["counter"]==300
    assert e.snapshot()["committed_operations"]==300
    assert e.snapshot()["sequence"]==300

def test_retry_30_percent_and_idempotency(tmp_path):
    e=ProcessingEngine(tmp_path/"x.wal")
    op={"op_id":"stable","key":"x","action":"add","value":1}
    result=deliver_with_retries(op,e.process,drop_rate=.30,seed=17)
    assert result["value"]==1
    # Simulated lost acknowledgement: repeat same ID; no second state mutation.
    again=e.process(op)
    assert again["duplicate"] is True
    assert e.snapshot()["state"]["x"]==1

def test_incomplete_tail_is_ignored(tmp_path):
    p=tmp_path/"x.wal"
    e=ProcessingEngine(p)
    e.process({"op_id":"1","key":"x","action":"add","value":2})
    with p.open("ab") as f: f.write(b'{"partial":')
    recovered=ProcessingEngine(p)
    assert recovered.snapshot()["state"]["x"]==2
