"""Local WebSocket tick publisher (exchange simulator).

Generates trades for several instruments (random walk on a 0.05 tick grid,
exchange timestamp = generation time, UTC, millisecond precision) and streams
them to every connected client that subscribed to the instrument. Optional
fault injection to exercise the consumer:

  --dup-prob P         re-send a tick (duplicate)
  --ooo-prob P         hold a tick back and send it after the next one (out of order)
  --invalid-prob P     send a malformed message
  --disconnect-every S close every client connection every S seconds

Run:  python -m market_data.publisher --port 8765
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import time
from typing import Iterable

from websockets.asyncio.server import Server, ServerConnection, broadcast, serve
from websockets.exceptions import ConnectionClosed

from .models import NS_PER_MS, format_ts

log = logging.getLogger(__name__)

DEFAULT_INSTRUMENTS = ("INFY", "TCS", "RELIANCE", "HDFCBANK", "NIFTY")
_START_PRICES = {"INFY": 1500.0, "TCS": 3500.0, "RELIANCE": 2500.0, "HDFCBANK": 1650.0, "NIFTY": 25999.5}


class TickPublisher:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8765,
        instruments: Iterable[str] = DEFAULT_INSTRUMENTS,
        rate: float = 200.0,
        seed: int | None = None,
        dup_prob: float = 0.0,
        ooo_prob: float = 0.0,
        invalid_prob: float = 0.0,
        disconnect_every: float = 0.0,
    ) -> None:
        self.host, self.port = host, port
        self.instruments = tuple(instruments)
        self.rate = rate
        self._rng = random.Random(seed)
        self.dup_prob, self.ooo_prob, self.invalid_prob = dup_prob, ooo_prob, invalid_prob
        self.disconnect_every = disconnect_every
        self._prices = {i: _START_PRICES.get(i, 1000.0) for i in self.instruments}
        self._subs: dict[ServerConnection, frozenset[str] | None] = {}
        self._server: Server | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self.sent = 0
        self.connections_served = 0

    async def start(self) -> "TickPublisher":
        self._server = await serve(self._handler, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]  # real port when 0 was asked
        self._tasks.append(asyncio.create_task(self._generate(), name="publisher-generate"))
        if self.disconnect_every > 0:
            self._tasks.append(asyncio.create_task(self._disconnector(), name="publisher-disconnect"))
        log.info("publisher listening on ws://%s:%d (%s, %.0f ticks/s)", self.host, self.port,
                 ",".join(self.instruments), self.rate)
        return self

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    async def _handler(self, ws: ServerConnection) -> None:
        self.connections_served += 1
        self._subs[ws] = frozenset()  # nothing until the client subscribes
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                if isinstance(msg, dict) and msg.get("op") == "subscribe":
                    inst = msg.get("instruments")
                    self._subs[ws] = None if inst == "*" else frozenset(map(str, inst or ()))
        except ConnectionClosed:
            pass
        finally:
            self._subs.pop(ws, None)

    def _next_tick(self) -> dict[str, object]:
        inst = self._rng.choice(self.instruments)
        p = self._prices[inst] + self._rng.choice((-2, -1, 0, 0, 1, 2)) * 0.05
        p = self._prices[inst] = round(max(0.05, p), 2)
        ts_ns = time.time_ns() // NS_PER_MS * NS_PER_MS  # exchange time, ms precision
        return {"instrument": inst, "timestamp": format_ts(ts_ns), "ltp": p, "ltq": self._rng.choice((1, 5, 10, 25, 50, 100))}

    def _send(self, tick: dict[str, object] | str, inst: str | None) -> None:
        msg = tick if isinstance(tick, str) else json.dumps(tick)
        targets = [ws for ws, want in self._subs.items() if want is None or (inst is not None and inst in want)]
        if targets:
            broadcast(targets, msg)  # non-blocking; a slow client cannot stall the others
            self.sent += 1

    async def _generate(self) -> None:
        interval = 1.0 / self.rate if self.rate > 0 else 1.0
        held: dict[str, object] | None = None
        next_at = time.monotonic()
        while True:
            next_at += interval
            delay = next_at - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            elif delay < -1:  # fell far behind (e.g. machine paused): do not burst
                next_at = time.monotonic()
            r = self._rng.random()
            if r < self.invalid_prob:
                self._send('{"instrument": "INFY", "ltp": "oops"}', "INFY")
                continue
            tick = self._next_tick()
            if held is None and self._rng.random() < self.ooo_prob:
                held = tick  # send it after the next one
                continue
            self._send(tick, tick["instrument"])  # type: ignore[arg-type]
            if held is not None:
                self._send(held, held["instrument"])  # type: ignore[arg-type]
                held = None
            if self._rng.random() < self.dup_prob:
                self._send(tick, tick["instrument"])  # type: ignore[arg-type]

    async def _disconnector(self) -> None:
        while True:
            await asyncio.sleep(self.disconnect_every)
            for ws in list(self._subs):
                log.info("publisher: dropping a client connection (fault injection)")
                await ws.close(code=1012, reason="publisher restart")

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()


def main() -> None:
    ap = argparse.ArgumentParser(description="Local WebSocket tick publisher (exchange simulator)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--instruments", default=",".join(DEFAULT_INSTRUMENTS))
    ap.add_argument("--rate", type=float, default=200.0, help="ticks per second, all instruments")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dup-prob", type=float, default=0.0)
    ap.add_argument("--ooo-prob", type=float, default=0.0)
    ap.add_argument("--invalid-prob", type=float, default=0.0)
    ap.add_argument("--disconnect-every", type=float, default=0.0, help="seconds; 0 = never")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    async def run() -> None:
        pub = TickPublisher(a.host, a.port, [s.strip() for s in a.instruments.split(",") if s.strip()], a.rate, a.seed,
                            a.dup_prob, a.ooo_prob, a.invalid_prob, a.disconnect_every)
        await pub.start()
        try:
            await asyncio.Event().wait()
        finally:
            await pub.stop()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
