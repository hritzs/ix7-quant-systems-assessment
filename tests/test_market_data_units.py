"""Market data module: parsing / validation, candle aggregation, CSV writer, bus."""
from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from market_data.bus import MarketDataBus
from market_data.candles import Candle, CandleAggregator, TickOutcome
from market_data.csv_writer import CSV_HEADER, CandleCsvWriter
from market_data.models import NS_PER_SECOND, Tick, TickValidationError, decode_message, format_ts, parse_tick, parse_ts


def tick(inst: str, ts: str, ltp: float, ltq: int = 1) -> Tick:
    return parse_tick({"instrument": inst, "timestamp": ts, "ltp": ltp, "ltq": ltq})


class TestParsing(unittest.TestCase):
    def test_example_tick_from_brief(self) -> None:
        t = parse_tick({"instrument": "INFY", "timestamp": "2026-11-17T09:15:00.125Z", "ltp": 25999.50, "ltq": 25})
        self.assertEqual((t.instrument, t.ltp, t.ltq), ("INFY", 25999.5, 25))
        self.assertEqual(t.timestamp, "2026-11-17T09:15:00.125Z")
        self.assertEqual(t.ts_ns % NS_PER_SECOND, 125_000_000)

    def test_offsets_are_converted_to_utc(self) -> None:
        self.assertEqual(parse_ts("2026-11-17T14:45:00.125+05:30"), parse_ts("2026-11-17T09:15:00.125Z"))

    def test_invalid_messages_are_rejected(self) -> None:
        bad = [
            {"instrument": "", "timestamp": "2026-11-17T09:15:00Z", "ltp": 1, "ltq": 1},
            {"instrument": "INFY", "timestamp": "2026-11-17T09:15:00", "ltp": 1, "ltq": 1},  # no timezone
            {"instrument": "INFY", "timestamp": "yesterday", "ltp": 1, "ltq": 1},
            {"instrument": "INFY", "timestamp": "2026-11-17T09:15:00Z", "ltp": "1", "ltq": 1},
            {"instrument": "INFY", "timestamp": "2026-11-17T09:15:00Z", "ltp": -5, "ltq": 1},
            {"instrument": "INFY", "timestamp": "2026-11-17T09:15:00Z", "ltp": float("nan"), "ltq": 1},
            {"instrument": "INFY", "timestamp": "2026-11-17T09:15:00Z", "ltp": 1, "ltq": 0},
            {"instrument": "INFY", "timestamp": "2026-11-17T09:15:00Z", "ltp": 1, "ltq": 2.5},
            {"instrument": "INFY", "timestamp": "2026-11-17T09:15:00Z", "ltp": True, "ltq": 1},
            ["not", "an", "object"],
        ]
        for obj in bad:
            with self.subTest(obj=obj), self.assertRaises(TickValidationError):
                parse_tick(obj)

    def test_frames_single_and_batched(self) -> None:
        self.assertEqual(len(decode_message('{"a": 1}')), 1)
        self.assertEqual(len(decode_message('[{"a": 1}, {"a": 2}]')), 2)
        with self.assertRaises(TickValidationError):
            decode_message("{not json")


class TestCandles(unittest.TestCase):
    def setUp(self) -> None:
        self.out: list[Candle] = []
        self.agg = CandleAggregator(self.out.append, allowed_lateness_ns=0)

    def test_ohlcv_and_minute_boundary(self) -> None:
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:00.000Z", 100, 10))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:20.000Z", 105, 5))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:40.000Z", 98, 7))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:59.999Z", 101, 3))  # still 09:15
        self.assertEqual(self.out, [])
        self.agg.on_tick(tick("INFY", "2026-11-17T09:16:00.000Z", 102, 1))  # opens 09:16, closes 09:15
        self.assertEqual(len(self.out), 1)
        c = self.out[0]
        self.assertEqual((c.start, c.end), ("2026-11-17T09:15:00.000Z", "2026-11-17T09:16:00.000Z"))
        self.assertEqual((c.open, c.high, c.low, c.close, c.volume, c.trades), (100, 105, 98, 101, 25, 4))

    def test_instruments_are_independent(self) -> None:
        for ts, inst, px, q in [("09:15:10", "INFY", 100, 1), ("09:15:11", "TCS", 200, 2), ("09:15:20", "INFY", 104, 3),
                                ("09:15:21", "TCS", 190, 4), ("09:15:30", "INFY", 99, 5), ("09:15:31", "TCS", 210, 6)]:
            self.agg.on_tick(tick(inst, f"2026-11-17T{ts}Z", px, q))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:16:01Z", 101))  # market time passes 09:16: both close
        got = {c.instrument: (c.open, c.high, c.low, c.close, c.volume, c.trades) for c in self.out}
        self.assertEqual(got, {"INFY": (100, 104, 99, 99, 9, 3), "TCS": (200, 210, 190, 210, 12, 3)})

    def test_out_of_order_within_open_candle_uses_exchange_time(self) -> None:
        agg = CandleAggregator(self.out.append, allowed_lateness_ns=2 * NS_PER_SECOND)
        agg.on_tick(tick("INFY", "2026-11-17T09:15:30Z", 100))
        agg.on_tick(tick("INFY", "2026-11-17T09:15:10Z", 90))  # earlier trade arrives late -> new open
        agg.on_tick(tick("INFY", "2026-11-17T09:15:50Z", 110))
        agg.on_tick(tick("INFY", "2026-11-17T09:15:40Z", 95))  # not the close: older than 09:15:50
        agg.on_tick(tick("INFY", "2026-11-17T09:16:01Z", 120))  # within lateness: 09:15 still open
        self.assertEqual(self.out, [])
        agg.on_tick(tick("INFY", "2026-11-17T09:15:59.500Z", 111))  # late but within lateness: accepted
        agg.on_tick(tick("INFY", "2026-11-17T09:16:02Z", 121))  # watermark reaches end + lateness
        c = self.out[0]
        self.assertEqual((c.open, c.high, c.low, c.close, c.trades), (90, 111, 90, 111, 5))
        self.assertEqual(agg.stats.out_of_order, 3)

    def test_late_tick_after_close_is_dropped(self) -> None:
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:10Z", 100))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:16:00Z", 101))
        self.assertEqual(self.agg.on_tick(tick("INFY", "2026-11-17T09:15:59Z", 99)), TickOutcome.LATE)
        self.assertEqual(self.agg.stats.late, 1)
        self.assertEqual(self.out[0].close, 100)  # written candle unchanged

    def test_duplicates_are_dropped(self) -> None:
        self.assertEqual(self.agg.on_tick(tick("INFY", "2026-11-17T09:15:10.5Z", 100, 5)), TickOutcome.ACCEPTED)
        self.assertEqual(self.agg.on_tick(tick("INFY", "2026-11-17T09:15:10.5Z", 100, 5)), TickOutcome.DUPLICATE)
        self.assertEqual(self.agg.on_tick(tick("INFY", "2026-11-17T09:15:10.5Z", 100, 6)), TickOutcome.ACCEPTED)
        self.assertEqual(self.agg.on_tick(tick("TCS", "2026-11-17T09:15:10.5Z", 100, 5)), TickOutcome.ACCEPTED)
        self.agg.flush()
        self.assertEqual(self.out[0].volume, 11)

    def test_quiet_instrument_closed_by_market_time(self) -> None:
        self.agg.on_tick(tick("ILLIQ", "2026-11-17T09:15:05Z", 50))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:30Z", 100))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:16:00.001Z", 100))  # market moved past 09:16
        self.assertEqual(sorted(c.instrument for c in self.out), ["ILLIQ", "INFY"])

    def test_shutdown_flush_marks_partial(self) -> None:
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:05Z", 50))
        self.assertEqual(self.agg.flush(), 1)
        self.assertEqual(self.out[0].status, "partial")
        self.assertEqual(self.agg.flush(), 0)

    def test_minutes_without_trades_produce_no_candle(self) -> None:
        self.agg.on_tick(tick("INFY", "2026-11-17T09:15:05Z", 50))
        self.agg.on_tick(tick("INFY", "2026-11-17T09:18:05Z", 51))
        self.assertEqual([c.start for c in self.out], ["2026-11-17T09:15:00.000Z"])


class TestCsvWriter(unittest.TestCase):
    def _candle(self, inst: str = "INFY", status: str = "closed") -> Candle:
        t = parse_ts("2026-11-17T09:15:00Z")
        return Candle(inst, t, t + 60 * NS_PER_SECOND, 1.5, 2.0, 1.0, 1.75, 30, 3, t, t, status)

    def test_schema_append_and_graceful_close(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "sub" / "candles.csv"  # parent folder is created
            w = CandleCsvWriter(path).start()
            for _ in range(500):
                w.write(self._candle())
            w.close()  # drains everything queued before returning
            w2 = CandleCsvWriter(path).start()  # reopen: append, header not repeated
            w2.write(self._candle("TCS", "partial"))
            w2.close()
            with path.open(newline="") as f:
                rows = list(csv.reader(f))
            self.assertEqual(rows[0], CSV_HEADER)
            self.assertEqual(len(rows), 502)
            self.assertEqual(rows[1], ["INFY", "2026-11-17T09:15:00.000Z", "2026-11-17T09:16:00.000Z",
                                       "1.5", "2.0", "1.0", "1.75", "30", "3", "closed"])
            self.assertEqual(rows[-1][-1], "partial")

    def test_refuses_file_with_another_schema(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "x.csv"
            path.write_text("a,b\n1,2\n")
            with self.assertRaises(ValueError):
                CandleCsvWriter(path).start()


class TestBus(unittest.IsolatedAsyncioTestCase):
    async def test_routing_and_interest_union(self) -> None:
        bus = MarketDataBus()
        changes: list[object] = []
        bus.on_interest_change(lambda: changes.append(bus.wanted_instruments()))
        a = bus.subscribe("a", ["INFY"])
        b = bus.subscribe("b", ["TCS", "INFY"])
        self.assertEqual(bus.wanted_instruments(), frozenset({"INFY", "TCS"}))
        t1 = tick("INFY", "2026-11-17T09:15:00Z", 1)
        t2 = tick("TCS", "2026-11-17T09:15:00Z", 2)
        t3 = tick("WIPRO", "2026-11-17T09:15:00Z", 3)
        for t in (t1, t2, t3):
            bus.publish(t)
        self.assertIs(await a.get(), t1)  # same object, no copy
        self.assertEqual([await b.get(), await b.get()], [t1, t2])
        self.assertEqual((a.backlog, b.backlog), (0, 0))  # WIPRO went nowhere
        c = bus.subscribe("all", None)
        self.assertIsNone(bus.wanted_instruments())
        bus.publish(t3)
        self.assertIs(await c.get(), t3)
        self.assertEqual(len(changes), 3)

    async def test_slow_subscriber_never_blocks_and_keeps_latest(self) -> None:
        bus = MarketDataBus()
        slow = bus.subscribe("slow", ["INFY"], maxsize=10)
        fast = bus.subscribe("fast", ["INFY"], maxsize=10_000)
        ticks = [tick("INFY", format_ts(parse_ts("2026-11-17T09:15:00Z") + i * 1_000_000), 100 + i) for i in range(1000)]
        for t in ticks:
            bus.publish(t)  # synchronous, never awaits: cannot block on a full queue
        self.assertEqual(slow.dropped, 990)
        self.assertEqual([(await slow.get()).ltp for _ in range(10)], [t.ltp for t in ticks[-10:]])
        self.assertEqual(fast.dropped, 0)
        self.assertEqual(fast.backlog, 1000)

    async def test_close_ends_iteration(self) -> None:
        bus = MarketDataBus()
        sub = bus.subscribe("s", ["INFY"])
        bus.publish(tick("INFY", "2026-11-17T09:15:00Z", 1))
        bus.close()
        got = [t async for t in sub]
        self.assertEqual(len(got), 1)
        self.assertTrue(sub.closed)


if __name__ == "__main__":
    unittest.main()
