"""Simulated exchange: records every dispatched action with its timestamps in
a CSV, and nothing else (no matching, no fills).

``dispatch`` is synchronous: the executor stamps the dispatch time, calls it,
and the exchange stamps its own arrival time in the same call, so the two
timestamps differ by microseconds and the CSV can be used to prove the rate
limit from the exchange's side (``verify_csv``).

It also keeps the set of live order ids so a MODIFY / CANCEL of an unknown or
already cancelled order is recorded as REJECTED (as a real exchange would).
"""
from __future__ import annotations

import csv
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .models import ActionType, OrderAction

EXCHANGE_CSV_HEADER = [
    "seq", "exchange_time_utc", "exchange_mono_ns", "dispatch_mono_ns", "action", "order_id",
    "instrument", "side", "quantity", "price", "producer", "exchange_status", "reason",
]


@dataclass(frozen=True, slots=True)
class ExchangeAck:
    seq: int
    exchange_mono_ns: int
    accepted: bool
    reason: str = ""


class SimulatedExchange:
    def __init__(self, csv_path: str | os.PathLike[str]) -> None:
        self.path = Path(csv_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("w", newline="", encoding="utf-8")
        self._csv = csv.writer(self._fh)
        self._csv.writerow(EXCHANGE_CSV_HEADER)
        self._fh.flush()
        self._lock = threading.Lock()  # dispatch is called from one task; the lock guards direct use from threads
        self._seq = 0
        self._live: set[str] = set()
        self.closed = False

    def dispatch(self, action: OrderAction, dispatch_mono_ns: int) -> ExchangeAck:
        with self._lock:
            if self.closed:
                raise RuntimeError("exchange connection closed")
            mono = time.monotonic_ns()
            wall = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
            self._seq += 1
            accepted, reason = True, ""
            if action.type is ActionType.NEW:
                if action.order_id in self._live:
                    accepted, reason = False, "duplicate order id"
                else:
                    self._live.add(action.order_id)
            elif action.order_id not in self._live:
                accepted, reason = False, "unknown or closed order"
            elif action.type is ActionType.CANCEL:
                self._live.discard(action.order_id)
            self._csv.writerow([
                self._seq, wall, mono, dispatch_mono_ns, action.type.value, action.order_id,
                action.instrument or "", action.side.value if action.side else "",
                "" if action.quantity is None else action.quantity, "" if action.price is None else action.price,
                action.producer, "ACK" if accepted else "REJECTED", reason,
            ])
            self._fh.flush()  # ~10 actions/s: flushing every record is cheap and keeps the log current
            return ExchangeAck(self._seq, mono, accepted, reason)

    def close(self) -> None:
        with self._lock:
            if not self.closed:
                self.closed = True
                self._fh.flush()
                os.fsync(self._fh.fileno())
                self._fh.close()


@dataclass(frozen=True, slots=True)
class VerifyReport:
    actions: int
    max_in_window: int
    limit: int
    worst_window_start_seq: int | None

    @property
    def ok(self) -> bool:
        return self.max_in_window <= self.limit


def max_in_rolling_window(times_ns: list[int], window_ns: int) -> tuple[int, int]:
    """Largest number of events inside any closed window of ``window_ns``
    (two pointers, O(n) after sorting). Returns (count, index of first event)."""
    ts = sorted(times_ns)
    best, best_i, lo = 0, -1, 0
    for hi, t in enumerate(ts):
        while t - ts[lo] > window_ns:
            lo += 1
        if hi - lo + 1 > best:
            best, best_i = hi - lo + 1, lo
    return best, best_i


def verify_csv(path: str | os.PathLike[str], limit: int = 10, window: float = 1.0,
               column: str = "exchange_mono_ns") -> VerifyReport:
    """Check the exchange log: no more than ``limit`` actions in any rolling window."""
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    times = [int(r[column]) for r in rows]
    count, i = max_in_rolling_window(times, int(window * 1_000_000_000))
    seq = None
    if i >= 0:
        order = sorted(range(len(times)), key=times.__getitem__)
        seq = int(rows[order[i]]["seq"])
    return VerifyReport(len(rows), count, limit, seq)
