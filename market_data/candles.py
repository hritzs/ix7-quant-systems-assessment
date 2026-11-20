"""One-minute OHLC aggregation on exchange time.

Per instrument, ticks are bucketed by ``ts_ns - ts_ns % interval`` (UTC
minutes). Design decisions (see README, "Duplicate and out-of-order ticks"):

* **Exchange time only.** Bucketing, ordering and closing a candle all use the
  exchange timestamps; the local clock is never consulted.
* **Open / close by timestamp, not arrival.** A tick that arrives out of order
  but belongs to a candle that is still open updates high / low / volume, and
  becomes the open if it is the earliest trade seen, or the close if it is the
  latest. Equal timestamps: the later arrival is the close.
* **Closing a candle.** A candle [start, end) is final once the instrument's
  watermark (latest exchange timestamp seen) reaches ``end + allowed_lateness``.
  A quiet instrument's candle is also closed when the *market* watermark (the
  latest exchange timestamp across all instruments) passes that point, so an
  illiquid instrument does not hold its candle open forever.
* **Late ticks.** A tick for a candle that has already been closed and written
  is dropped and counted (``late``); written rows are never rewritten.
* **Duplicates.** The tick format has no trade id, so a duplicate is a tick
  identical in (timestamp, price, quantity) to one of the instrument's last
  ``dedupe_size`` ticks. This catches re-sent frames and replays after a
  reconnect; the trade-off (two genuine identical trades in the same
  millisecond would count once) is documented and can be disabled.
* **No trades, no candle.** Minutes without trades produce no row.
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from .models import NS_PER_MINUTE, NS_PER_MS, Tick, format_ts


class TickOutcome(Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    LATE = "late"


@dataclass(slots=True)
class Candle:
    instrument: str
    start_ns: int
    end_ns: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    trades: int
    open_ts_ns: int
    close_ts_ns: int
    status: str = "closed"  # "closed" or "partial" (flushed at shutdown)

    @property
    def start(self) -> str:
        return format_ts(self.start_ns)

    @property
    def end(self) -> str:
        return format_ts(self.end_ns)


@dataclass(slots=True)
class AggregatorStats:
    accepted: int = 0
    duplicates: int = 0
    late: int = 0
    out_of_order: int = 0  # accepted, but older than the instrument's watermark
    candles_closed: int = 0


class _Instrument:
    __slots__ = ("open", "closed_through", "watermark", "seen")

    def __init__(self) -> None:
        self.open: dict[int, Candle] = {}  # start_ns -> candle (1, sometimes 2 within lateness)
        self.closed_through = -1  # start_ns of the newest closed candle
        self.watermark = -1  # latest exchange ts seen
        self.seen: OrderedDict[tuple[int, float, int], None] = OrderedDict()


class CandleAggregator:
    """Single-threaded by design: it is driven from the ingestion task, so it
    needs no locks. Every operation is O(1) per tick (amortised)."""

    def __init__(
        self,
        on_candle: Callable[[Candle], None],
        interval_ns: int = NS_PER_MINUTE,
        allowed_lateness_ns: int = 500 * NS_PER_MS,
        dedupe_size: int = 4096,
    ) -> None:
        if interval_ns <= 0:
            raise ValueError("interval must be positive")
        self._on_candle = on_candle
        self._interval = interval_ns
        self._lateness = max(0, allowed_lateness_ns)
        self._dedupe_size = dedupe_size
        self._instruments: dict[str, _Instrument] = {}
        self.market_watermark = -1
        self._idle_checked = -1  # last candle boundary the idle sweep ran for
        self.stats = AggregatorStats()

    def on_tick(self, tick: Tick) -> TickOutcome:
        st = self._instruments.get(tick.instrument)
        if st is None:
            st = self._instruments[tick.instrument] = _Instrument()

        if self._dedupe_size:
            fp = (tick.ts_ns, tick.ltp, tick.ltq)
            if fp in st.seen:
                self.stats.duplicates += 1
                return TickOutcome.DUPLICATE
            st.seen[fp] = None
            if len(st.seen) > self._dedupe_size:
                st.seen.popitem(last=False)

        start = tick.ts_ns - tick.ts_ns % self._interval
        if start <= st.closed_through:
            self.stats.late += 1
            return TickOutcome.LATE

        candle = st.open.get(start)
        if candle is None:
            st.open[start] = Candle(
                instrument=tick.instrument, start_ns=start, end_ns=start + self._interval,
                open=tick.ltp, high=tick.ltp, low=tick.ltp, close=tick.ltp,
                volume=tick.ltq, trades=1, open_ts_ns=tick.ts_ns, close_ts_ns=tick.ts_ns,
            )
        else:
            if tick.ltp > candle.high:
                candle.high = tick.ltp
            if tick.ltp < candle.low:
                candle.low = tick.ltp
            candle.volume += tick.ltq
            candle.trades += 1
            if tick.ts_ns < candle.open_ts_ns:
                candle.open, candle.open_ts_ns = tick.ltp, tick.ts_ns
            if tick.ts_ns >= candle.close_ts_ns:
                candle.close, candle.close_ts_ns = tick.ltp, tick.ts_ns

        self.stats.accepted += 1
        if tick.ts_ns < st.watermark:
            self.stats.out_of_order += 1
        else:
            st.watermark = tick.ts_ns
            self._close_ready(st, tick.ts_ns)
        if tick.ts_ns > self.market_watermark:
            self.market_watermark = tick.ts_ns
            self._close_idle()
        return TickOutcome.ACCEPTED

    def _close_ready(self, st: _Instrument, watermark: int) -> None:
        if not st.open:
            return
        ready = [s for s in st.open if s + self._interval + self._lateness <= watermark]
        for s in sorted(ready):
            self._emit(st, st.open.pop(s), "closed")

    def _close_idle(self) -> None:
        # Cheap: runs only when the market watermark crosses a candle boundary.
        wm = self.market_watermark
        boundary = wm - self._lateness
        boundary -= boundary % self._interval
        if boundary <= self._idle_checked:
            return
        self._idle_checked = boundary
        for st in self._instruments.values():
            self._close_ready(st, wm)

    def _emit(self, st: _Instrument, candle: Candle, status: str) -> None:
        candle.status = status
        if candle.start_ns > st.closed_through:
            st.closed_through = candle.start_ns
        self.stats.candles_closed += 1
        self._on_candle(candle)

    def flush(self) -> int:
        """Shutdown: emit every open candle as ``partial`` (the minute was not
        over). Returns how many were emitted."""
        n = 0
        for st in self._instruments.values():
            for s in sorted(st.open):
                self._emit(st, st.open.pop(s), "partial")
                n += 1
        return n

    def open_candles(self) -> list[Candle]:
        return [c for st in self._instruments.values() for c in st.open.values()]
