"""Order executor: limiter semantics, the collective limit under concurrent
producers (asyncio tasks and OS threads), coalescing and lifecycle."""
from __future__ import annotations

import asyncio
import csv
import random
import tempfile
import threading
import unittest
from pathlib import Path

from order_executor import (ActionType, SimulatedExchange, SlidingWindowRateLimiter, Status, TradeOrderExecutor,
                            cancel_order, max_in_rolling_window, modify_order, new_order, verify_csv)

S = 1_000_000_000


class FakeClock:
    def __init__(self) -> None:
        self.t = 1_000 * S

    def __call__(self) -> int:
        return self.t


class TestSlidingWindow(unittest.TestCase):
    def test_eleventh_waits_for_the_window(self) -> None:
        clk = FakeClock()
        lim = SlidingWindowRateLimiter(10, 1.0, safety_margin=0.0, clock_ns=clk)
        for i in range(10):
            clk.t += 10_000_000  # 10 actions, 10 ms apart
            self.assertIsNotNone(lim.try_acquire())
        first = 1_000 * S + 10_000_000
        self.assertIsNone(lim.try_acquire())
        clk.t = first + S  # exactly 1 s after the first: same closed window -> still refused
        self.assertIsNone(lim.try_acquire())
        self.assertEqual(lim.next_slot_ns(), first + S + 1)
        clk.t = first + S + 1
        self.assertIsNotNone(lim.try_acquire())

    def test_no_double_burst_across_a_second_boundary(self) -> None:
        clk = FakeClock()
        lim = SlidingWindowRateLimiter(10, 1.0, clock_ns=clk)
        clk.t = 1_000 * S + 990_000_000  # 10 at x.990
        for _ in range(10):
            self.assertIsNotNone(lim.try_acquire())
        clk.t = 1_001 * S + 10_000_000  # x+1.010: a fixed-window counter would allow 10 more here
        self.assertIsNone(lim.try_acquire())

    def test_verifier(self) -> None:
        self.assertEqual(max_in_rolling_window([0, S, 2 * S], S)[0], 2)  # closed window
        self.assertEqual(max_in_rolling_window([i * 100_000_000 for i in range(11)], S)[0], 11)  # 0..1.0 s


class ExecutorCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.csv = Path(self._tmp.name) / "exchange.csv"

    async def asyncTearDown(self) -> None:
        self._tmp.cleanup()

    def rows(self) -> list[dict[str, str]]:
        with self.csv.open(newline="") as f:
            return list(csv.DictReader(f))


class TestExecutorLimit(ExecutorCase):
    async def test_burst_of_mixed_actions_is_spread_over_windows(self) -> None:
        ex = await TradeOrderExecutor(SimulatedExchange(self.csv), coalesce=False).start()
        futs = []
        for i in range(10):
            futs.append(ex.submit(new_order(f"N{i}", "INFY", "BUY", 1, 100.0)))
        for i in range(5):
            futs.append(ex.submit(modify_order(f"N{i}", price=101.0)))
        for i in range(5, 10):
            futs.append(ex.submit(cancel_order(f"N{i}")))
        res = await asyncio.gather(*futs)  # 20 actions: two windows
        await ex.stop()
        self.assertTrue(all(r.status is Status.DISPATCHED for r in res))
        rep = verify_csv(self.csv)
        self.assertEqual((rep.actions, rep.max_in_window), (20, 10))
        disp = sorted(r.dispatch_mono_ns for r in res)
        self.assertGreater(disp[10] - disp[0], S)  # the 11th left only after the window
        self.assertEqual([r["action"] for r in self.rows()], ["NEW"] * 10 + ["MODIFY"] * 5 + ["CANCEL"] * 5)  # FIFO

    async def test_concurrent_async_and_thread_producers_cannot_exceed_limit(self) -> None:
        # 10 per 0.25 s keeps the test fast; the logic is identical to 10 per second.
        lim = SlidingWindowRateLimiter(10, 0.25)
        ex = await TradeOrderExecutor(SimulatedExchange(self.csv), lim, coalesce=False).start()
        n_async, n_threads, per = 6, 6, 8
        start = asyncio.Event()
        barrier = threading.Barrier(n_threads)

        async def async_producer(p: int) -> list:
            await start.wait()  # all producers fire at the same moment
            futs = [ex.submit(new_order(f"A{p}-{i}", "TCS", "SELL", 1, 10.0, f"a{p}")) for i in range(per)]
            return await asyncio.gather(*futs)

        def thread_producer(p: int) -> list:
            barrier.wait()
            futs = [ex.submit_threadsafe(new_order(f"T{p}-{i}", "INFY", "BUY", 1, 10.0, f"t{p}")) for i in range(per)]
            return [f.result(timeout=30) for f in futs]

        tasks = [asyncio.create_task(async_producer(p)) for p in range(n_async)]
        threads = [asyncio.to_thread(thread_producer, p) for p in range(n_threads)]
        start.set()
        results = [r for group in await asyncio.gather(*tasks, *threads) for r in group]
        await ex.stop()

        total = (n_async + n_threads) * per
        self.assertEqual(len(results), total)
        self.assertTrue(all(r.status is Status.DISPATCHED for r in results))
        rep = verify_csv(self.csv, limit=10, window=0.25)
        self.assertEqual(rep.actions, total)
        self.assertLessEqual(rep.max_in_window, 10)
        rep_disp = verify_csv(self.csv, limit=10, window=0.25, column="dispatch_mono_ns")
        self.assertLessEqual(rep_disp.max_in_window, 10)
        self.assertEqual(len({r.exchange_seq for r in results}), total)  # each action sent exactly once
        rows = self.rows()  # per-producer order is preserved
        for p in range(n_async):
            ids = [r["order_id"] for r in rows if r["producer"] == f"a{p}"]
            self.assertEqual(ids, [f"A{p}-{i}" for i in range(per)])

    async def test_producers_do_not_block_while_queued(self) -> None:
        ex = await TradeOrderExecutor(SimulatedExchange(self.csv), SlidingWindowRateLimiter(10, 60)).start()
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        futs = [ex.submit(new_order(f"Q{i}", "INFY", "BUY", 1, 1.0)) for i in range(50)]
        self.assertLess(loop.time() - t0, 0.05)  # submitting 50 is instant even though 40 must wait a minute
        await asyncio.sleep(0.05)
        self.assertEqual(sum(f.done() for f in futs), 10)
        await ex.stop(drain=False)
        self.assertEqual(sum(f.result().status is Status.SHUTDOWN for f in futs), 40)
        self.assertEqual(len(self.rows()), 10)


class TestCoalescing(ExecutorCase):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        # A full window: everything submitted now stays queued, so coalescing is deterministic.
        self.ex = await TradeOrderExecutor(SimulatedExchange(self.csv), SlidingWindowRateLimiter(10, 3600)).start()
        for i in range(10):
            await self.ex.place("INFY", "BUY", 1, 1.0, order_id=f"F{i}")

    async def test_modify_merges_into_queued_new(self) -> None:
        f_new = self.ex.submit(new_order("X", "INFY", "BUY", 5, 100.0))
        f_mod = self.ex.submit(modify_order("X", price=101.0))
        await asyncio.sleep(0)
        self.assertFalse(f_new.done() or f_mod.done())  # waiting for the window
        await self.ex.stop(drain=False)
        self.assertEqual(f_new.result().status, Status.SHUTDOWN)

    async def test_cancel_of_queued_new_sends_nothing(self) -> None:
        f_new = self.ex.submit(new_order("X", "INFY", "BUY", 5, 100.0))
        f_mod = self.ex.submit(modify_order("X", price=101.0))
        f_can = self.ex.submit(cancel_order("X"))
        for f in (f_new, f_mod, f_can):
            self.assertEqual((await f).status, Status.CANCELLED_BEFORE_SEND)
        self.assertEqual(self.ex.pending, 0)
        self.assertEqual(self.ex.submit(modify_order("X", price=1.0)).result().status, Status.REJECTED)  # gone
        await self.ex.stop(drain=False)

    async def test_newer_modify_supersedes_older_and_cancel_drops_modifies(self) -> None:
        f1 = self.ex.submit(modify_order("F0", price=2.0))
        f2 = self.ex.submit(modify_order("F0", price=3.0))
        self.assertEqual((await f1).status, Status.SUPERSEDED)
        self.assertFalse(f2.done())
        f3 = self.ex.submit(cancel_order("F0"))
        self.assertEqual((await f2).status, Status.SUPERSEDED)
        self.assertEqual(self.ex.pending, 1)  # only the cancel remains
        self.assertFalse(f3.done())
        self.assertEqual(self.ex.submit(cancel_order("F0")).result().status, Status.REJECTED)  # already cancelling
        await self.ex.stop(drain=False)

    async def test_local_rejections(self) -> None:
        self.assertEqual(self.ex.submit(modify_order("nope", price=1.0)).result().status, Status.REJECTED)
        self.assertEqual(self.ex.submit(new_order("F1", "INFY", "BUY", 1, 1.0)).result().status, Status.REJECTED)
        await self.ex.stop(drain=False)
        self.assertEqual(self.ex.submit(new_order("Z", "INFY", "BUY", 1, 1.0)).result().status, Status.REJECTED)


class TestDrainAndMergedSend(ExecutorCase):
    async def test_drain_sends_merged_new_once(self) -> None:
        ex = await TradeOrderExecutor(SimulatedExchange(self.csv), SlidingWindowRateLimiter(2, 0.2)).start()
        futs = [ex.submit(new_order(f"D{i}", "INFY", "BUY", 1, 1.0)) for i in range(4)]
        f_mod = ex.submit(modify_order("D3", price=9.5, quantity=7))  # merged into the queued NEW
        await ex.stop(drain=True)
        self.assertTrue(all(f.result().status is Status.DISPATCHED for f in futs))
        self.assertEqual(f_mod.result().status, Status.COALESCED)
        rows = self.rows()
        self.assertEqual(len(rows), 4)
        self.assertEqual((rows[-1]["action"], rows[-1]["price"], rows[-1]["quantity"]), ("NEW", "9.5", "7"))
        self.assertEqual(f_mod.result().sent.type, ActionType.NEW)

    async def test_random_mixed_load_never_violates(self) -> None:
        rng = random.Random(3)
        ex = await TradeOrderExecutor(SimulatedExchange(self.csv), SlidingWindowRateLimiter(10, 0.2)).start()
        live: list[str] = []
        futs = []
        for i in range(150):
            r = rng.random()
            if r < 0.5 or not live:
                oid = f"R{i}"
                live.append(oid)
                futs.append(ex.submit(new_order(oid, "INFY", "BUY", 1, 1.0)))
            elif r < 0.8:
                futs.append(ex.submit(modify_order(rng.choice(live), price=round(rng.uniform(1, 2), 2))))
            else:
                futs.append(ex.submit(cancel_order(live.pop(rng.randrange(len(live))))))
            if rng.random() < 0.3:
                await asyncio.sleep(rng.uniform(0, 0.03))
        await ex.stop(drain=True)
        self.assertTrue(all(f.done() for f in futs))
        rep = verify_csv(self.csv, limit=10, window=0.2)
        self.assertLessEqual(rep.max_in_window, 10)
        self.assertTrue(all(r["exchange_status"] == "ACK" for r in self.rows()))  # causality kept per order


if __name__ == "__main__":
    unittest.main()
