"""Reproducible synthetic operation generation."""
import uuid

def make_operation(index: int, *, key_count: int = 100, action: str = "add") -> dict:
    return {"op_id": f"op-{index:012d}", "key": f"key-{index % key_count}",
            "action": action, "value": 1.0, "trace_id": uuid.uuid5(uuid.NAMESPACE_OID, str(index)).hex}
