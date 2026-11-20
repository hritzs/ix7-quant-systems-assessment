"""Fan-out of ticks from the single upstream connection to many strategies.

* One upstream WebSocket, one parse per tick: every subscriber receives the
  same immutable ``Tick`` object (no copies, no re-parsing).
* Routing is precomputed: ``instrument -> tuple of subscriptions`` is rebuilt
  only when someone subscribes / unsubscribes, so ``publish`` is a dict lookup
  plus one ``put_nowait`` per interested subscriber.
* Each strategy has its own bounded queue and runs in its own task, so a slow
  strategy can never block ingestion or the other strategies. When a queue is
  full the **oldest** tick is dropped (conflation: a strategy that falls
  behind gets the most recent prices) and the drop is counted.
* The bus also tells the feed client which instruments anyone wants, so the
  upstream subscription is the union of the strategies' interests.
"""
from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import AsyncIterator, Callable

from .models import Tick

_CLOSED = object()


class Subscription:
    def __init__(self, name: str, instruments: frozenset[str] | None, maxsize: int) -> None:
        self.name = name
        self.instruments = instruments  # None = every instrument
        self._queue: asyncio.Queue[object] = asyncio.Queue(maxsize)
        self.delivered = 0
        self.dropped = 0
        self.closed = False

    def _offer(self, item: object) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._queue.get_nowait()  # drop the oldest
            self.dropped += 1
            self._queue.put_nowait(item)
        if item is not _CLOSED:
            self.delivered += 1

    async def get(self) -> Tick | None:
        """Next tick, or None once the bus is closed."""
        item = await self._queue.get()
        if item is _CLOSED:
            self.closed = True
            return None
        return item  # type: ignore[return-value]

    def __aiter__(self) -> AsyncIterator[Tick]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[Tick]:
        while True:
            tick = await self.get()
            if tick is None:
                return
            yield tick

    @property
    def backlog(self) -> int:
        return self._queue.qsize()


class MarketDataBus:
    """Used from the event-loop thread only (single writer: no locks)."""

    def __init__(self, default_maxsize: int = 10_000) -> None:
        self._default_maxsize = default_maxsize
        self._subs: list[Subscription] = []
        self._routes: dict[str, tuple[Subscription, ...]] = {}
        self._wildcard: tuple[Subscription, ...] = ()
        self._listeners: list[Callable[[], None]] = []
        self.published = 0

    def subscribe(self, name: str, instruments: Iterable[str] | None = None, maxsize: int | None = None) -> Subscription:
        inst = None if instruments is None else frozenset(i.strip() for i in instruments)
        sub = Subscription(name, inst, maxsize or self._default_maxsize)
        self._subs.append(sub)
        self._rebuild()
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub in self._subs:
            self._subs.remove(sub)
            sub._offer(_CLOSED)
            self._rebuild()

    def on_interest_change(self, fn: Callable[[], None]) -> None:
        self._listeners.append(fn)

    def wanted_instruments(self) -> frozenset[str] | None:
        """Union of the subscriptions; None means every instrument."""
        if self._wildcard:
            return None
        return frozenset(i for s in self._subs for i in (s.instruments or ()))

    def _rebuild(self) -> None:
        routes: dict[str, list[Subscription]] = {}
        for s in self._subs:
            for i in s.instruments or ():
                routes.setdefault(i, []).append(s)
        self._wildcard = tuple(s for s in self._subs if s.instruments is None)
        self._routes = {i: tuple(v) + self._wildcard for i, v in routes.items()}
        for fn in self._listeners:
            fn()

    def publish(self, tick: Tick) -> None:
        self.published += 1
        for sub in self._routes.get(tick.instrument, self._wildcard):
            sub._offer(tick)

    def close(self) -> None:
        for s in self._subs:
            s._offer(_CLOSED)

    @property
    def subscriptions(self) -> tuple[Subscription, ...]:
        return tuple(self._subs)
