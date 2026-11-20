"""Market data module end to end: local publisher -> WebSocket -> app -> CSV,
including reconnects, fault injection and graceful shutdown."""
from __future__ import annotations

import asyncio
import csv
import json
import tempfile
import unittest
from pathlib import Path

from websockets.asyncio.server import serve

from market_data.app import MarketDataApp, MarketDataConfig, StrategySpec
from market_data.client import Backoff, FeedClient
from market_data.csv_writer import CSV_HEADER
from market_data.publisher import TickPublisher


async def wait_until(cond, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not cond():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


class TestEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def test_feed_to_strategies_and_csv_with_faults_and_reconnect(self) -> None:
        pub = await TickPublisher(port=0, rate=300, seed=1, dup_prob=0.05, ooo_prob=0.05,
                                  invalid_prob=0.01, disconnect_every=1.0).start()
        with tempfile.TemporaryDirectory() as d:
            out = Path(d) / "candles.csv"
            app = MarketDataApp(MarketDataConfig(
                url=pub.url, csv_path=out, interval_seconds=1, allowed_lateness_ms=200,
                strategies=[StrategySpec("infy", ["INFY"]), StrategySpec("all", None)],
                backoff=Backoff(initial=0.05, maximum=0.2)))
            await app.start()
            await wait_until(lambda: app.client.stats.connects >= 3, timeout=8)  # survived 2+ drops
            await app.stop()
            await pub.stop()

            f, a = app.client.stats, app.aggregator.stats
            self.assertGreater(f.ticks, 200)
            self.assertGreater(f.invalid, 0)  # malformed messages were counted and dropped
            self.assertGreater(a.duplicates, 0)
            self.assertEqual(a.accepted + a.duplicates + a.late, f.ticks)  # every tick accounted for
            infy, everything = app.strategies
            self.assertEqual(set(infy.vwap), {"INFY"})  # only what it subscribed to
            self.assertEqual(everything.ticks, a.accepted + a.late)  # duplicates never reach strategies

            with out.open(newline="") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(list(rows[0].keys()), CSV_HEADER)
            self.assertGreater(len(rows), 5)
            self.assertEqual(sum(int(r["trade_count"]) for r in rows), a.accepted)  # nothing lost at shutdown
            for r in rows:
                self.assertLessEqual(float(r["low"]), min(float(r["open"]), float(r["close"])))
                self.assertGreaterEqual(float(r["high"]), max(float(r["open"]), float(r["close"])))
            self.assertIn("partial", {r["status"] for r in rows})  # open candles flushed at shutdown

    async def test_client_reconnects_after_server_restart_and_resubscribes(self) -> None:
        subs: list[object] = []

        async def handler(ws):
            async for raw in ws:
                msg = json.loads(raw)
                subs.append(msg["instruments"])
                await ws.send(json.dumps({"instrument": "INFY", "timestamp": "2026-11-17T09:15:00.125Z",
                                          "ltp": 25999.5, "ltq": 25}))

        server = await serve(handler, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        got: list[object] = []
        client = FeedClient(f"ws://127.0.0.1:{port}", got.append, lambda: frozenset({"INFY"}),
                            Backoff(initial=0.05, maximum=0.2))
        task = asyncio.create_task(client.run())
        await wait_until(lambda: len(got) == 1)
        server.close()  # exchange goes away...
        await server.wait_closed()
        await asyncio.sleep(0.3)  # ...client retries with backoff meanwhile
        server = await serve(handler, "127.0.0.1", port)  # ...and comes back
        await wait_until(lambda: len(got) == 2, timeout=8)
        await client.stop()
        await asyncio.wait_for(task, 5)
        server.close()
        await server.wait_closed()
        self.assertEqual(subs, [["INFY"], ["INFY"]])  # subscription re-sent on reconnect
        self.assertGreaterEqual(client.stats.connects, 2)

    async def test_stop_while_disconnected_is_prompt(self) -> None:
        client = FeedClient("ws://127.0.0.1:9", lambda t: None, backoff=Backoff(initial=5, maximum=5))
        task = asyncio.create_task(client.run())
        await asyncio.sleep(0.2)  # first connect fails, now in a 5 s backoff
        await client.stop()
        await asyncio.wait_for(task, 1.0)  # interruptible sleep: no 5 s wait


if __name__ == "__main__":
    unittest.main()
