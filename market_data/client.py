"""Single upstream WebSocket connection with reconnects.

Protocol with the publisher (``publisher.py``):
  client -> server  {"op": "subscribe", "instruments": ["INFY", ...]}  or  "instruments": "*"
  server -> client  one tick object per frame, or a JSON array of ticks

Reconnects: exponential backoff with full jitter (0.1 s doubling to 5 s by
default), reset after a connection that delivered data; the subscription is
re-sent on every connect. Dead connections are detected by WebSocket
ping / pong (``ping_interval`` / ``ping_timeout``), not only by TCP errors.

The receive loop does only cheap, non-blocking work per frame (JSON decode,
validation, then the ``on_tick`` callback, which updates the in-memory candle
and puts the tick on the strategies' queues). Anything slow (CSV I/O,
strategy logic) runs elsewhere, so the socket is always drained promptly.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
from dataclasses import dataclass
from typing import Callable

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

from .models import Tick, TickValidationError, decode_message, parse_tick

log = logging.getLogger(__name__)


@dataclass
class Backoff:
    initial: float = 0.1
    maximum: float = 5.0
    factor: float = 2.0
    _attempt: int = 0

    def next_delay(self) -> float:
        cap = min(self.maximum, self.initial * (self.factor ** self._attempt))
        self._attempt += 1
        return random.uniform(0, cap) if self._attempt > 1 else cap  # first retry quick, then full jitter

    def reset(self) -> None:
        self._attempt = 0


@dataclass
class FeedStats:
    connects: int = 0
    disconnects: int = 0
    frames: int = 0
    ticks: int = 0
    invalid: int = 0


class FeedClient:
    def __init__(
        self,
        url: str,
        on_tick: Callable[[Tick], None],
        wanted_instruments: Callable[[], frozenset[str] | None] = lambda: None,
        backoff: Backoff | None = None,
        ping_interval: float | None = 5.0,
        ping_timeout: float | None = 5.0,
        open_timeout: float = 5.0,
    ) -> None:
        self.url = url
        self._on_tick = on_tick
        self._wanted = wanted_instruments
        self._backoff = backoff or Backoff()
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._open_timeout = open_timeout
        self._ws: ClientConnection | None = None
        self._stopping = asyncio.Event()
        self.connected = asyncio.Event()
        self.stats = FeedStats()
        self._invalid_logged = 0

    async def run(self) -> None:
        """Connect and consume until ``stop()``; reconnect on any failure."""
        while not self._stopping.is_set():
            try:
                async with connect(
                    self.url, open_timeout=self._open_timeout, ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout, close_timeout=2, max_queue=1024,
                ) as ws:
                    self._ws = ws
                    self.stats.connects += 1
                    await self._send_subscription(ws)
                    self.connected.set()
                    log.info("connected to %s", self.url)
                    got_data = False
                    async for raw in ws:
                        if not got_data:
                            got_data = True
                            self._backoff.reset()
                        self._handle(raw)
            except asyncio.CancelledError:
                raise
            except InvalidURI:
                raise
            except (OSError, ConnectionClosed, InvalidHandshake, TimeoutError) as exc:
                if not self._stopping.is_set():
                    log.warning("feed connection lost: %s", exc)
            finally:
                if self._ws is not None:
                    self.stats.disconnects += 1
                self._ws = None
                self.connected.clear()
            if self._stopping.is_set():
                break
            delay = self._backoff.next_delay()
            log.info("reconnecting in %.2fs", delay)
            try:  # interruptible sleep: stop() ends it at once
                await asyncio.wait_for(self._stopping.wait(), delay)
            except TimeoutError:
                pass

    def _handle(self, raw: str | bytes) -> None:
        self.stats.frames += 1
        try:
            objs = decode_message(raw)
        except TickValidationError as exc:
            self._invalid(exc, raw)
            return
        for obj in objs:
            try:
                tick = parse_tick(obj)
            except TickValidationError as exc:
                self._invalid(exc, obj)
                continue
            self.stats.ticks += 1
            self._on_tick(tick)

    def _invalid(self, exc: Exception, payload: object) -> None:
        self.stats.invalid += 1
        if self._invalid_logged < 20:  # never flood the log from a bad feed
            self._invalid_logged += 1
            log.warning("invalid message dropped (%s): %.200r", exc, payload)

    async def _send_subscription(self, ws: ClientConnection) -> None:
        wanted = self._wanted()
        instruments: object = "*" if wanted is None else sorted(wanted)
        await ws.send(json.dumps({"op": "subscribe", "instruments": instruments}))

    def resubscribe(self) -> None:
        """Call when strategies' interests change; sends the new union."""
        ws = self._ws
        if ws is not None:
            asyncio.get_running_loop().create_task(self._safe_send_subscription(ws))

    async def _safe_send_subscription(self, ws: ClientConnection) -> None:
        try:
            await self._send_subscription(ws)
        except ConnectionClosed:
            pass  # the next connect sends it

    async def stop(self) -> None:
        self._stopping.set()
        ws = self._ws
        if ws is not None:
            await ws.close()
