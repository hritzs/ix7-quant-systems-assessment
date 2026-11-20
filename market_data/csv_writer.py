"""Candle persistence to CSV on a dedicated writer thread.

File I/O blocks, so it never runs on the event loop: ``write()`` only puts the
candle on a thread-safe queue (O(1), never blocks), and the writer thread
appends rows. The thread writes everything queued, then flushes once (a burst
of candles at a minute boundary costs one flush, not one per row). ``close()``
drains the queue, flushes and fsyncs, so a graceful shutdown loses nothing.
"""
from __future__ import annotations

import csv
import logging
import os
import queue
import threading
from pathlib import Path

from .candles import Candle

log = logging.getLogger(__name__)

CSV_HEADER = [
    "instrument", "interval_start", "interval_end",
    "open", "high", "low", "close", "volume", "trade_count", "status",
]

_STOP = object()


def candle_row(c: Candle) -> list[str]:
    return [
        c.instrument, c.start, c.end,
        repr(c.open), repr(c.high), repr(c.low), repr(c.close),
        str(c.volume), str(c.trades), c.status,
    ]


class CandleCsvWriter:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self._q: queue.SimpleQueue[object] = queue.SimpleQueue()
        self._thread: threading.Thread | None = None
        self.rows_written = 0
        self._error: BaseException | None = None

    def start(self) -> "CandleCsvWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        new_file = not self.path.exists() or self.path.stat().st_size == 0
        if not new_file:
            with self.path.open("r", newline="", encoding="utf-8") as f:
                header = next(csv.reader(f), None)
            if header != CSV_HEADER:
                raise ValueError(f"{self.path} exists with a different header: {header}")
        self._fh = self.path.open("a", newline="", encoding="utf-8")
        self._csv = csv.writer(self._fh)
        if new_file:
            self._csv.writerow(CSV_HEADER)
            self._fh.flush()
        self._thread = threading.Thread(target=self._run, name="candle-csv-writer", daemon=True)
        self._thread.start()
        return self

    def write(self, candle: Candle) -> None:
        """Thread-safe and non-blocking."""
        if self._thread is None:
            raise RuntimeError("writer not started")
        self._q.put(candle_row(candle))

    def _run(self) -> None:
        try:
            while True:
                item = self._q.get()
                stop = item is _STOP
                if not stop:
                    self._csv.writerow(item)  # type: ignore[arg-type]
                    self.rows_written += 1
                # drain whatever else is already queued, then flush once
                while not stop:
                    try:
                        item = self._q.get_nowait()
                    except queue.Empty:
                        break
                    if item is _STOP:
                        stop = True
                        break
                    self._csv.writerow(item)  # type: ignore[arg-type]
                    self.rows_written += 1
                self._fh.flush()
                if stop:
                    os.fsync(self._fh.fileno())
                    return
        except BaseException as exc:  # surface I/O errors to close()
            self._error = exc
            log.exception("candle CSV writer failed")

    def close(self, timeout: float | None = 10.0) -> None:
        if self._thread is None:
            return
        self._q.put(_STOP)
        self._thread.join(timeout)
        self._fh.close()
        self._thread = None
        if self._error is not None:
            raise RuntimeError("candle CSV writer failed") from self._error
