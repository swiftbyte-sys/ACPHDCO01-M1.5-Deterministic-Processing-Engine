"""Lossy transport simulator with bounded retry and idempotent delivery."""
import random, time
from typing import Callable

class DeliveryError(RuntimeError):
    pass

def deliver_with_retries(operation: dict, handler: Callable[[dict], dict], *,
                         drop_rate: float = 0.30, max_attempts: int = 100,
                         seed: int | None = None, base_delay: float = 0.0001,
                         max_delay: float = 0.01) -> dict:
    """Simulate packet loss before receiver; retries use decorrelated jitter.

    Since the simulated drop occurs before handler invocation, a dropped packet
    is not committed. If an ACK is notionally lost after commit, replaying the
    same op_id is safe because the engine is idempotent.
    """
    if not 0 <= drop_rate < 1:
        raise ValueError("drop_rate must be in [0, 1)")
    rng = random.Random(seed)
    previous = base_delay
    for attempt in range(1, max_attempts + 1):
        if rng.random() >= drop_rate:
            result = handler(operation)
            result["delivery_attempts"] = attempt
            return result
        if attempt < max_attempts:
            delay = min(max_delay, rng.uniform(base_delay, max(base_delay, previous * 3)))
            time.sleep(delay)
            previous = delay
    raise DeliveryError(f"delivery failed after {max_attempts} attempts")
