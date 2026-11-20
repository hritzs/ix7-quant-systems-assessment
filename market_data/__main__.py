"""Run the market data module.

  python -m market_data --demo --duration 130          # publisher + consumer in one process
  python -m market_data.publisher --port 8765          # terminal 1
  python -m market_data --url ws://127.0.0.1:8765       # terminal 2 (Ctrl+C to stop)
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
from pathlib import Path

from .app import MarketDataApp, MarketDataConfig, StrategySpec
from .publisher import TickPublisher


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m market_data", description="Live market data module")
    ap.add_argument("--url", default="ws://127.0.0.1:8765", help="WebSocket feed URL")
    ap.add_argument("--out", default="output/candles.csv", help="candle CSV path")
    ap.add_argument("--interval", type=int, default=60, help="candle length in seconds (default 60)")
    ap.add_argument("--lateness-ms", type=int, default=500, help="how long a candle waits for out-of-order ticks")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run; 0 = until Ctrl+C")
    ap.add_argument("--demo", action="store_true", help="also start a local publisher in this process")
    ap.add_argument("--demo-faults", action="store_true", help="demo publisher injects duplicates, out-of-order, "
                    "invalid messages and a disconnect every 20 s")
    ap.add_argument("--slow-strategy", action="store_true", help="add a deliberately slow strategy (backpressure demo)")
    ap.add_argument("-v", "--verbose", action="store_true")
    return ap.parse_args()


async def amain(a: argparse.Namespace) -> None:
    pub = None
    url = a.url
    if a.demo:
        f = a.demo_faults
        pub = await TickPublisher(port=0, seed=7, dup_prob=0.02 if f else 0, ooo_prob=0.02 if f else 0,
                                  invalid_prob=0.005 if f else 0, disconnect_every=20 if f else 0).start()
        url = pub.url
    strategies = [StrategySpec("vwap-INFY-TCS", ["INFY", "TCS"]), StrategySpec("vwap-all", None)]
    if a.slow_strategy:
        strategies.append(StrategySpec("slow-NIFTY", ["NIFTY"], processing_delay=0.05, queue_size=100))
    app = MarketDataApp(MarketDataConfig(url=url, csv_path=Path(a.out), interval_seconds=a.interval,
                                         allowed_lateness_ms=a.lateness_ms, strategies=strategies))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, AttributeError, ValueError):  # Windows: no signal handlers
            loop.add_signal_handler(sig, stop.set)

    await app.start()
    logging.info("consuming %s -> %s (Ctrl+C to stop)", url, a.out)
    try:
        if a.duration > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), a.duration)
        else:
            await stop.wait()
    finally:  # also runs when Ctrl+C cancels the task (Windows)
        await app.stop()
        if pub is not None:
            await pub.stop()
        print(app.summary())
        print(f"candles written to {Path(a.out).resolve()}")


def main() -> None:
    a = parse_args()
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("websockets").setLevel(logging.WARNING)
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(amain(a))


if __name__ == "__main__":
    main()
