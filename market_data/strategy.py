"""Sample strategies. Each runs in its own task and only reads its own queue,
so it is isolated from the feed and from the other strategies."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from .bus import Subscription
from .models import Tick

log = logging.getLogger(__name__)


@dataclass
class VwapStrategy:
    """Tracks a running VWAP per instrument and logs when the last price
    crosses it. Not a trading algorithm -- it shows the subscription API."""

    name: str
    log_every: int = 0  # 0 = never log individual signals
    processing_delay: float = 0.0  # >0 simulates a slow strategy (backpressure demo)
    ticks: int = 0
    vwap: dict[str, float] = field(default_factory=dict)
    _pv: dict[str, float] = field(default_factory=dict)
    _vol: dict[str, int] = field(default_factory=dict)
    _side: dict[str, int] = field(default_factory=dict)
    crosses: int = 0

    def on_tick(self, t: Tick) -> None:
        self.ticks += 1
        pv = self._pv.get(t.instrument, 0.0) + t.ltp * t.ltq
        vol = self._vol.get(t.instrument, 0) + t.ltq
        self._pv[t.instrument], self._vol[t.instrument] = pv, vol
        vwap = self.vwap[t.instrument] = pv / vol
        side = 1 if t.ltp > vwap else -1
        prev = self._side.get(t.instrument)
        self._side[t.instrument] = side
        if prev is not None and prev != side:
            self.crosses += 1
            if self.log_every and self.crosses % self.log_every == 0:
                log.info("[%s] %s crossed %s VWAP %.2f at %.2f (%s)", self.name, t.instrument,
                         "above" if side > 0 else "below", vwap, t.ltp, t.timestamp)

    async def run(self, sub: Subscription) -> None:
        async for tick in sub:
            self.on_tick(tick)
            if self.processing_delay:
                await asyncio.sleep(self.processing_delay)
