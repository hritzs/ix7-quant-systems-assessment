"""Run the Trade Order Executor under concurrent load and verify the log.

  python -m order_executor                                   # 4 async + 3 thread producers, ~80 actions
  python -m order_executor --async-producers 8 --thread-producers 8 --orders-per-producer 5
  python -m order_executor --verify output/exchange_log.csv  # re-check an existing log
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import concurrent.futures
import logging
import random
import threading
import time
from pathlib import Path

from . import (ActionResult, SimulatedExchange, SlidingWindowRateLimiter, Status, TradeOrderExecutor,
               cancel_order, modify_order, new_order, verify_csv)

INSTRUMENTS = ("INFY", "TCS", "RELIANCE", "HDFCBANK")


def _plan(rng: random.Random, name: str, n_orders: int, ids: list[str]) -> list:
    """A realistic mix per producer: place, then modify some, cancel some."""
    actions = []
    for oid in ids[:n_orders]:
        px = round(rng.uniform(1000, 3000), 1)
        actions.append(new_order(oid, rng.choice(INSTRUMENTS), rng.choice(("BUY", "SELL")), rng.choice((1, 5, 10)), px, name))
        if rng.random() < 0.6:
            actions.append(modify_order(oid, price=round(px * rng.uniform(0.99, 1.01), 1), producer=name))
        if rng.random() < 0.5:
            actions.append(cancel_order(oid, producer=name))
    return actions


async def run(a: argparse.Namespace) -> int:
    out = Path(a.out)
    exchange = SimulatedExchange(out)
    executor = await TradeOrderExecutor(exchange, SlidingWindowRateLimiter(a.limit, a.window),
                                        coalesce=not a.no_coalesce).start()
    rng = random.Random(a.seed)
    results: list[ActionResult] = []
    lock = threading.Lock()

    async def async_producer(i: int) -> None:
        name = f"async-{i}"
        ids = [executor.new_order_id() for _ in range(a.orders_per_producer)]
        futs = []
        for act in _plan(random.Random(rng.random()), name, a.orders_per_producer, ids):
            futs.append(executor.submit(act))
            await asyncio.sleep(random.uniform(0, 0.02))  # producers fire at their own pace
        results.extend(await asyncio.gather(*futs))

    def thread_producer(i: int, ids: list[str]) -> None:
        name = f"thread-{i}"
        futs = [executor.submit_threadsafe(act) for act in _plan(random.Random(i * 7919 + a.seed), name, len(ids), ids)]
        done = [f.result(timeout=600) for f in futs]
        with lock:
            results.extend(done)

    t0 = time.monotonic()
    thread_ids = [[executor.new_order_id() for _ in range(a.orders_per_producer)] for _ in range(a.thread_producers)]
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, a.thread_producers))
    loop = asyncio.get_running_loop()
    thread_jobs = [loop.run_in_executor(pool, thread_producer, i, thread_ids[i]) for i in range(a.thread_producers)]
    await asyncio.gather(*(async_producer(i) for i in range(a.async_producers)), *thread_jobs)
    await executor.stop(drain=True)
    pool.shutdown()
    elapsed = time.monotonic() - t0

    by_status = collections.Counter(r.status.value for r in results)
    report = verify_csv(out, a.limit, a.window)
    waits = sorted(r.queue_wait_s for r in results if r.status in (Status.DISPATCHED, Status.COALESCED))
    print(f"producers: {a.async_producers} asyncio + {a.thread_producers} threads, {len(results)} requests in {elapsed:.1f}s")
    print("results:", ", ".join(f"{k}={v}" for k, v in sorted(by_status.items())))
    print(f"exchange actions: {report.actions} (slots saved by coalescing: {executor.stats.saved_by_coalescing})")
    if waits:
        print(f"queue wait: median {waits[len(waits) // 2]:.3f}s, max {waits[-1]:.3f}s")
    print(f"max actions in any rolling {a.window:g}s window: {report.max_in_window} (limit {a.limit}) -> "
          f"{'OK' if report.ok else 'VIOLATION'}")
    print(f"exchange log: {out.resolve()}")
    return 0 if report.ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m order_executor", description="Rate-limited trade order executor demo")
    ap.add_argument("--out", default="output/exchange_log.csv")
    ap.add_argument("--async-producers", type=int, default=4)
    ap.add_argument("--thread-producers", type=int, default=3)
    ap.add_argument("--orders-per-producer", type=int, default=6)
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--window", type=float, default=1.0)
    ap.add_argument("--no-coalesce", action="store_true", help="send every action as submitted (pure FIFO)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--verify", metavar="CSV", help="only verify an existing exchange log")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if a.verify:
        r = verify_csv(a.verify, a.limit, a.window)
        print(f"{r.actions} actions; max in any rolling {a.window:g}s window: {r.max_in_window} -> {'OK' if r.ok else 'VIOLATION'}")
        raise SystemExit(0 if r.ok else 1)
    raise SystemExit(asyncio.run(run(a)))


if __name__ == "__main__":
    main()
