"""Wiring of the market data module and its lifecycle.

  WebSocket ──► FeedClient ──► on_tick ──► CandleAggregator ──► CandleCsvWriter (thread)
                (1 connection)      │
                                    └────► MarketDataBus ──► strategy queues ──► strategy tasks

The candle aggregator runs inline on the ingestion path (O(1) per tick, never
lossy); strategies are fed through bounded queues (isolated, may conflate).
Duplicates are not passed to strategies; late ticks (real trades for an
already-written candle) are.

Shutdown order (``stop``): stop the feed -> close open candles as ``partial``
-> close the bus (strategies finish their queues) -> drain, flush and fsync
the CSV writer.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path

from .bus import MarketDataBus
from .candles import CandleAggregator, TickOutcome
from .client import Backoff, FeedClient
from .csv_writer import CandleCsvWriter
from .models import NS_PER_MS, NS_PER_SECOND, Tick
from .strategy import VwapStrategy

log = logging.getLogger(__name__)


@dataclass
class StrategySpec:
    name: str
    instruments: list[str] | None  # None = all
    processing_delay: float = 0.0
    queue_size: int = 10_000


@dataclass
class MarketDataConfig:
    url: str = "ws://127.0.0.1:8765"
    csv_path: Path = Path("output/candles.csv")
    interval_seconds: int = 60
    allowed_lateness_ms: int = 500
    dedupe_size: int = 4096
    strategies: list[StrategySpec] = field(default_factory=lambda: [
        StrategySpec("vwap-INFY-TCS", ["INFY", "TCS"]),
        StrategySpec("vwap-all", None),
    ])
    backoff: Backoff = field(default_factory=Backoff)
    flush_partial_on_shutdown: bool = True


class MarketDataApp:
    def __init__(self, cfg: MarketDataConfig) -> None:
        self.cfg = cfg
        self.bus = MarketDataBus()
        self.writer = CandleCsvWriter(cfg.csv_path)
        self.aggregator = CandleAggregator(
            on_candle=self.writer.write,
            interval_ns=cfg.interval_seconds * NS_PER_SECOND,
            allowed_lateness_ns=cfg.allowed_lateness_ms * NS_PER_MS,
            dedupe_size=cfg.dedupe_size,
        )
        self.client = FeedClient(cfg.url, self._on_tick, self.bus.wanted_instruments, cfg.backoff)
        self.strategies: list[VwapStrategy] = []
        self._tasks: list[asyncio.Task[None]] = []

    def _on_tick(self, tick: Tick) -> None:
        if self.aggregator.on_tick(tick) is not TickOutcome.DUPLICATE:
            self.bus.publish(tick)

    async def start(self) -> None:
        self.writer.start()
        for spec in self.cfg.strategies:
            strat = VwapStrategy(spec.name, processing_delay=spec.processing_delay)
            sub = self.bus.subscribe(spec.name, spec.instruments, spec.queue_size)
            self.strategies.append(strat)
            self._tasks.append(asyncio.create_task(strat.run(sub), name=f"strategy-{spec.name}"))
        self.bus.on_interest_change(self.client.resubscribe)
        self._tasks.append(asyncio.create_task(self.client.run(), name="feed-client"))

    async def stop(self) -> None:
        await self.client.stop()
        feed = self._tasks[-1] if self._tasks else None
        if feed is not None:
            try:
                await asyncio.wait_for(feed, 5)
            except (TimeoutError, asyncio.CancelledError):
                feed.cancel()
            except Exception:  # the feed task had failed: still shut the rest down cleanly
                log.exception("feed client ended with an error")
        if self.cfg.flush_partial_on_shutdown:
            n = self.aggregator.flush()
            if n:
                log.info("flushed %d open candle(s) as partial", n)
        self.bus.close()
        await asyncio.gather(*self._tasks[:-1], return_exceptions=True)
        await asyncio.to_thread(self.writer.close)

    def summary(self) -> str:
        a, f = self.aggregator.stats, self.client.stats
        lines = [
            f"feed: connects={f.connects} frames={f.frames} ticks={f.ticks} invalid={f.invalid}",
            f"candles: accepted={a.accepted} duplicates={a.duplicates} out_of_order={a.out_of_order} "
            f"late_dropped={a.late} candles_written={self.writer.rows_written}",
        ]
        for s, sub in zip(self.strategies, self.bus.subscriptions):
            lines.append(f"strategy {s.name}: ticks={s.ticks} dropped={sub.dropped} vwap_crosses={s.crosses} "
                         f"vwap={ {k: round(v, 2) for k, v in sorted(s.vwap.items())} }")
        return "\n".join(lines)
