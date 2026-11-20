# IX7 Quant Systems Engineering — Technical Assessment

Two independent Python modules for quantitative trading infrastructure:

| Module | Package | What it does |
|---|---|---|
| **1. Live Market Data** | `market_data/` | Consumes ticks over one WebSocket connection, fans them out to multiple strategies, builds one-minute OHLC candles on **exchange time**, persists completed candles to CSV. Includes a local WebSocket publisher (exchange simulator) with fault injection. |
| **2. Exchange Rate Limiter** | `order_executor/` | Trade Order Executor for new / modify / cancel actions under a strict **collective limit of 10 actions in any rolling second**, safe under concurrent producers (asyncio tasks and OS threads). A simulated exchange records every dispatched action with timestamps in a CSV. |

The modules share no code and run, and are tested, independently.

---

## Requirements and setup

* **Python 3.11 or later.** The test suite was run on Python 3.11.18 and 3.14.4 (Linux). The code is written for Windows too: `pathlib` paths, CSVs opened with `newline=""` and UTF-8, no POSIX-only calls, and SIGINT / SIGTERM handlers are optional (on Windows Ctrl+C cancels the main task and the same graceful shutdown runs).
* **One third-party dependency:** `websockets==15.0.1` (module 1 only). The executor and all tests use only the standard library.
* No database, no API keys, no external services: storage is CSV and in-memory.

```bash
git clone <repo-url> ix7 && cd ix7
python -m venv .venv
# Linux / macOS                     # Windows (PowerShell)
source .venv/bin/activate           # .venv\Scripts\Activate.ps1
pip install -r requirements.txt     # or: pip install -e .
```

## Running each module independently

### Module 1 — Live Market Data

One process (publisher and consumer together, the quickest way to see it work):

```bash
python -m market_data --demo --duration 130                  # ~2 full minutes of candles
python -m market_data --demo --demo-faults --slow-strategy   # + duplicates, out-of-order, invalid
                                                             #   messages, a disconnect every 20 s,
                                                             #   and a deliberately slow strategy
```

Two processes (a real separate feed):

```bash
python -m market_data.publisher --port 8765 --rate 200            # terminal 1: exchange simulator
python -m market_data --url ws://127.0.0.1:8765 --out output/candles.csv   # terminal 2; Ctrl+C stops gracefully
```

Options: `--interval` (candle seconds, default 60), `--lateness-ms` (default 500), `--duration` (0 = until Ctrl+C).
Publisher fault injection: `--dup-prob`, `--ooo-prob`, `--invalid-prob`, `--disconnect-every SECONDS`.

### Module 2 — Trade Order Executor

```bash
python -m order_executor                      # 4 asyncio + 3 thread producers, mixed new/modify/cancel
python -m order_executor --no-coalesce        # every request sent as submitted (pure FIFO)
python -m order_executor --async-producers 8 --thread-producers 8 --orders-per-producer 10
python -m order_executor --verify output/exchange_log.csv   # re-check any exchange log
```

The run ends by reading the exchange CSV back and checking the largest number of actions in any rolling one-second window.

## Running the tests

```bash
python -m unittest discover -s tests -t . -v
```

32 tests, ~8 s. They cover:

| Area | Tests |
|---|---|
| Parsing / validation | the brief's example tick; timezone offsets; 10 kinds of invalid message; single and batched frames |
| Candles | OHLCV and the exact minute boundary (09:15:59.999 vs 09:16:00.000); independent instruments; out-of-order ticks inside an open candle (open / close by exchange time); late ticks after a candle closed; duplicates; quiet instruments closed by market time; shutdown flush (`partial`); minutes without trades |
| CSV | schema, header written once, append to an existing file, refusal of a file with another schema, drain-on-close (500 rows) |
| Distribution | routing per instrument, union of interests, one shared tick object, a slow subscriber never blocks and keeps the latest ticks, close ends iteration |
| End to end | publisher → WebSocket → app → CSV with injected duplicates / out-of-order / invalid messages and forced disconnects; every tick accounted for; no tick lost at shutdown; reconnect to a restarted server with the subscription re-sent; prompt stop while in backoff |
| Rate limiter | the 11th action waits; the closed-window boundary (exactly 1.000 s is still refused); no double burst across a second boundary; the verifier |
| Executor concurrency | 6 asyncio + 6 thread producers released at the same instant; limit holds by the exchange's own timestamps; each action sent once; per-producer order kept; producers never block; random mixed load of 150 actions |
| Lifecycle / coalescing | modify merged into a queued new; cancel of a queued new sends nothing; newer modify supersedes older; cancel drops queued modifies; local rejections; drain vs no-drain shutdown |

---

## Module 1 — how it works

```
 local publisher ──ws──► FeedClient ──► on_tick ──► CandleAggregator ──► CandleCsvWriter ──► candles.csv
 (exchange sim)        (1 connection)      │         (inline, O(1))      (own thread)
                                           └──► MarketDataBus ──► bounded queue ──► strategy task  (×N)
```

| Component | Responsibility |
|---|---|
| `models.py` | `Tick` (immutable, slots), JSON frame decoding (one tick or an array), validation, ISO-8601 ⇄ integer nanoseconds |
| `client.py` — `FeedClient` | The single upstream connection; subscribes to the union of the strategies' instruments; reconnects with exponential backoff and jitter; detects dead connections with WebSocket ping/pong; counts and drops invalid messages without flooding the log |
| `candles.py` — `CandleAggregator` | Per-instrument one-minute OHLCV on exchange time; duplicate, out-of-order and late handling; closing rules |
| `csv_writer.py` — `CandleCsvWriter` | Appends rows on a dedicated thread; batch flush; fsync and drain on close |
| `bus.py` — `MarketDataBus` | Fan-out to strategies: precomputed routes, one bounded queue per strategy, drop-oldest when full |
| `strategy.py` — `VwapStrategy` | Sample strategy (running VWAP per instrument, counts crosses); optional processing delay to demonstrate backpressure |
| `publisher.py` — `TickPublisher` | Local WebSocket exchange simulator: random-walk trades on a 0.05 grid, per-client subscriptions, fault injection |
| `app.py` / `__main__.py` | Wiring, lifecycle, CLI, graceful shutdown |

### Design decisions

**Concurrency model: one asyncio event loop + one writer thread.** The work per tick is tiny and I/O-bound on the socket, so a single event loop avoids locks entirely: the aggregator and the bus are only touched from the ingestion task. The one blocking operation, file I/O, runs on its own thread behind a thread-safe queue, so a slow disk never stalls the socket. Strategies run as independent tasks.

**Never block market data processing.** The receive loop does decode → validate → aggregate → `put_nowait` to strategy queues, all O(1) and non-awaiting. Nothing in that path can wait on a strategy or on the disk.

**One upstream connection, one parse.** Strategies subscribe on the bus; the client subscribes upstream to the *union* of their instruments (re-sent automatically when interests change or after a reconnect). Each tick is parsed once and the same immutable object is handed to every strategy that wants it.

**Slow strategies are isolated.** Each strategy has its own bounded queue. When it falls behind, the oldest tick is dropped and counted (conflation: a lagging strategy acts on the latest prices, and memory stays bounded). Strategies that need every tick can be given a larger queue. The alternatives (blocking the feed, or unbounded queues) either stall everyone or grow without limit.

**Candles are lossless.** The aggregator runs inline, not as a bus subscriber, so it never drops ticks, whatever the strategies do.

**Reconnects.** Any connection failure → exponential backoff (first retry after 0.1 s, then full jitter on a cap doubling to 5 s, so many clients do not reconnect in lockstep), reset once a connection delivers data. The backoff sleep is interruptible, so shutdown is immediate even while disconnected. Ping/pong (5 s) detects half-open connections that TCP would not report.

### Duplicate and out-of-order ticks (exchange time)

| Case | Decision |
|---|---|
| Bucketing | `start = ts − ts mod 60 s` on the exchange timestamp in UTC (integer nanoseconds, so 09:15:59.999 and 09:16:00.000 never round into the wrong minute). The local clock is never used. |
| Out of order, candle still open | Accepted. High / low / volume / trade count update; the **open** is the trade with the earliest exchange timestamp and the **close** the one with the latest (an equal timestamp: the later arrival is the close). Arrival order does not decide open and close. |
| When a candle closes | When the instrument's watermark (latest exchange timestamp seen) reaches `end + allowed_lateness` (default 500 ms, `--lateness-ms`). The lateness window is the trade-off between publishing candles quickly and accepting slightly delayed ticks. |
| Quiet instruments | Also closed when the **market** watermark (latest exchange timestamp across all instruments) passes `end + allowed_lateness`, so an illiquid instrument does not hold its candle open until its next trade. Still exchange time, never the local clock. If one instrument's ticks routinely arrive later than the others' by more than the lateness, raise `--lateness-ms`. |
| Late, candle already closed | Dropped and counted (`late_dropped`). A written row is never rewritten: consumers of the CSV can rely on rows being final. Late ticks still reach strategies (they are real trades). |
| Duplicates | The tick has no trade id, so a duplicate is a tick identical in (timestamp, price, quantity) to one of the instrument's last 4,096 ticks (bounded LRU). This catches re-sent frames and replays after a reconnect. Trade-off: two genuine identical trades in the same millisecond count once (`dedupe_size=0` disables it). Duplicates reach neither candles nor strategies. |
| Minutes without trades | No row (no synthetic flat candles). |
| Disconnection | Candles are not closed by the local clock while the feed is down: we cannot know whether trades happened. They close when exchange time moves on after reconnecting, or at shutdown. |

### CSV schema and persistence

```
instrument,interval_start,interval_end,open,high,low,close,volume,trade_count,status
```

* Times are ISO-8601 UTC with milliseconds; `interval_end` is exclusive. Prices are written with full precision (`repr`).
* `status` is `closed` for a completed minute, or `partial` for a candle still open at shutdown (flushed so no traded volume is lost, and marked so it is never mistaken for a full minute).
* The file and its folder are created if missing. An existing file with the same header is appended to (the header is not repeated); one with a different header is refused, never silently mixed.
* Rows are written by the writer thread; after each burst it flushes once (one flush per minute boundary, not per row). `close()` drains the queue, flushes and `fsync`s.
* Graceful shutdown (Ctrl+C / SIGTERM / `--duration`): stop the feed → flush open candles as `partial` → close the bus (strategies finish their queues) → drain, flush and fsync the CSV.

### Example output

A 75-second run with fault injection (a disconnect every 20 s, ~2 % duplicates and out-of-order ticks, invalid messages) and a deliberately slow strategy:

```
$ python -m market_data --demo --demo-faults --slow-strategy --duration 75
... INFO market_data.publisher: publisher: dropping a client connection (fault injection)
... WARNING market_data.client: feed connection lost: received 1012 (service restart) publisher restart
... INFO market_data.client: reconnecting in 0.10s
... INFO market_data.client: connected to ws://127.0.0.1:38395
... INFO market_data.app: flushed 5 open candle(s) as partial
feed: connects=4 frames=15220 ticks=15156 invalid=64
candles: accepted=14875 duplicates=281 out_of_order=71 late_dropped=0 candles_written=10
strategy vwap-INFY-TCS: ticks=6044 dropped=0 vwap_crosses=71 vwap={'INFY': 1497.95, 'TCS': 3502.26}
strategy vwap-all: ticks=14875 dropped=0 vwap_crosses=198 vwap={'HDFCBANK': 1651.28, 'INFY': 1497.95, 'NIFTY': 25996.89, 'RELIANCE': 2502.33, 'TCS': 3502.26}
strategy slow-NIFTY: ticks=1582 dropped=1350 vwap_crosses=14 vwap={'NIFTY': 25996.96}
candles written to ./output/candles.csv
```

Every tick is accounted for (14,875 accepted + 281 duplicates = 15,156), three disconnects were each recovered in 0.1 s, and the slow strategy shed 1,350 old ticks without slowing the feed or the other strategies.

`output/candles.csv` (the first minute starts when the run started, so it holds only the ticks received since connecting; the last minute was still open at shutdown):

```
instrument,interval_start,interval_end,open,high,low,close,volume,trade_count,status
TCS,2026-11-20T08:27:00.000Z,2026-11-20T08:28:00.000Z,3500.0,3504.25,3499.0,3503.5,75549,2363,closed
NIFTY,2026-11-20T08:27:00.000Z,2026-11-20T08:28:00.000Z,25999.4,26000.05,25994.45,25994.85,72761,2241,closed
INFY,2026-11-20T08:27:00.000Z,2026-11-20T08:28:00.000Z,1499.95,1500.35,1495.7,1496.8,73074,2263,closed
RELIANCE,2026-11-20T08:27:00.000Z,2026-11-20T08:28:00.000Z,2500.0,2504.15,2499.8,2503.8,74998,2262,closed
HDFCBANK,2026-11-20T08:27:00.000Z,2026-11-20T08:28:00.000Z,1649.95,1654.75,1648.85,1652.4,73684,2305,closed
TCS,2026-11-20T08:28:00.000Z,2026-11-20T08:29:00.000Z,3503.45,3504.25,3502.8,3503.65,22733,704,partial
...
```

---

## Module 2 — how it works

```
 producers (asyncio tasks) ──submit()──────────┐
 producers (OS threads) ──submit_threadsafe()──┤  call_soon_threadsafe
                                               ▼
                         FIFO queue (+ per-order index for coalescing)          event-loop thread only
                                               │
                              single dispatcher task ── owns ──► SlidingWindowRateLimiter
                                               │
                                               ▼
                         SimulatedExchange.dispatch() ──► exchange_log.csv (seq, timestamps, action…)
```

| Component | Responsibility |
|---|---|
| `models.py` | `OrderAction` (NEW / MODIFY / CANCEL), `ActionResult`, `Status` |
| `rate_limiter.py` — `SlidingWindowRateLimiter` | Exact rolling-window limit: at most N events in any window of W seconds |
| `executor.py` — `TradeOrderExecutor` | Producer APIs (asyncio and thread-safe), the queue, coalescing, the single dispatcher, lifecycle (drain / no-drain stop) |
| `exchange.py` — `SimulatedExchange` | Records each dispatched action with sequence, wall-clock UTC and monotonic timestamps to CSV; tracks live order ids to reject a modify / cancel of an unknown order; `verify_csv` checks a log |
| `__main__.py` | Concurrent-load demo with asyncio and thread producers, then verification |

### Design decisions

**Exact sliding window, not a fixed window or token bucket.** "10 in any rolling second" is enforced literally: an action at time *t* is allowed only if the 10th most recent dispatch was **strictly more than** 1 s before *t* (the window is closed, so an action exactly 1.000 s later is still refused). A fixed per-second counter would allow 20 actions within ~20 ms across a second boundary. A token bucket refilling at 10/s can also exceed 10 within some rolling second. The sliding log keeps only the last 10 dispatch times (`deque(maxlen=10)`): O(1) time and memory.

**One dispatcher owns the limiter (single-writer).** Every action from every producer goes through one queue to one dispatcher task, and only that task reads or updates the limiter. Two producers can never both see "9 sent" and both send: the 11th action is impossible by construction, without any lock or check-then-act race. Thread producers hand over with `loop.call_soon_threadsafe`, so even the queue is only ever touched by the event-loop thread.

**No polling, no unnecessary blocking.** Producers never wait for the limit: `submit()` returns a future at once (50 submissions with 40 queued take under 50 ms in the tests). The dispatcher sleeps exactly until the next slot opens, or until work arrives, and re-checks the limiter with the real monotonic clock after waking (an event-loop timer can fire up to one clock tick early, and Windows timers are coarse). Excess requests are queued to the next window, in order.

**Timestamps that prove the limit from the exchange's side.** The dispatcher stamps the time, calls `dispatch` (synchronous: the exchange stamps its own arrival time inside the call), then records the time **after** the call returned in the limiter. That is an upper bound on the exchange's stamp, so the limit also holds by the exchange's own clock even if a GC pause happened inside the call. A 1 ms safety margin absorbs residual jitter. Monotonic clocks are used for all limit arithmetic (wall clock can jump); the CSV also has wall-clock UTC for readability.

**FIFO preserves causality.** Actions go out in submission order, so an order's NEW is always sent before its MODIFY and CANCEL.

**Coalescing saves slots without breaking causality** (on by default, `--no-coalesce` disables it). A rate-limited system should not spend slots on actions that are already obsolete:

| Situation | Result |
|---|---|
| MODIFY while the order's NEW is still queued | Merged into the NEW (one slot instead of two); the modify's caller gets `COALESCED` |
| MODIFY while another MODIFY for the order is queued | The queued one takes the newer price / quantity and keeps its queue position; the older caller gets `SUPERSEDED` |
| CANCEL while the order's NEW is still queued | Nothing is sent; all callers get `CANCELLED_BEFORE_SEND` (zero slots, and the order never reaches the market) |
| CANCEL with modifies queued | The modifies are dropped (`SUPERSEDED`); only the cancel is sent |
| MODIFY / CANCEL of an unknown order, a second CANCEL, a duplicate order id | Rejected locally, costs no slot |

**Lifecycle.** `stop(drain=True)` sends everything already queued (at the rate limit) and then closes the exchange log (flush + fsync); `stop(drain=False)` resolves queued actions as `SHUTDOWN` without sending them. After stop, new submissions are rejected.

**Prioritisation (not implemented, deliberately).** Letting cancels jump the queue reduces risk faster, but it can reorder an order's own actions (a cancel overtaking its NEW). The coalescing rules already give cancels of queued orders zero latency; a priority lane would need per-order sequencing and is noted as a possible extension.

### Simulated exchange CSV

```
seq,exchange_time_utc,exchange_mono_ns,dispatch_mono_ns,action,order_id,instrument,side,quantity,price,producer,exchange_status,reason
1,2026-11-20T08:27:28.103937Z,18022294770152,18022294768839,NEW,O000001,INFY,SELL,1,2278.9,thread-0,ACK,
```

### Example output

Default run (coalescing on; producers fire faster than the limit, so many orders are cancelled while still queued):

```
$ python -m order_executor
producers: 4 asyncio + 3 threads, 85 requests in 2.0s
results: CANCELLED_BEFORE_SEND=37, COALESCED=19, DISPATCHED=29
exchange actions: 29 (slots saved by coalescing: 56)
queue wait: median 0.990s, max 1.968s
max actions in any rolling 1s window: 10 (limit 10) -> OK
exchange log: .../output/exchange_log.csv
```

Pure FIFO (every request sent): exactly 10 per second, queued into the following windows:

```
$ python -m order_executor --no-coalesce
producers: 4 asyncio + 3 threads, 85 requests in 8.0s
results: DISPATCHED=85
exchange actions: 85 (slots saved by coalescing: 0)
queue wait: median 3.986s, max 7.896s
max actions in any rolling 1s window: 10 (limit 10) -> OK

actions per elapsed second (from the exchange log): {0: 10, 1: 10, 2: 10, 3: 10, 4: 10, 5: 10, 6: 10, 7: 10, 8: 5}
```

---

## Performance considerations

| Path | Cost (measured, one core, this machine) |
|---|---|
| Decode + validate one tick | ~2.3 µs |
| Candle aggregation | ~0.9 µs per tick |
| Fan-out to 3 strategies | ~0.4 µs per tick |
| **Ingestion total** | **~276,000 ticks/s on one core** |
| Rate limiter decision | O(1), compares against the oldest of the last 10 timestamps |
| Executor | one dispatch per slot; producers never block; dispatcher sleeps until the next slot (no busy-wait) |

* Integer nanosecond timestamps everywhere: exact boundaries, cheap comparisons.
* `slots=True` dataclasses, precomputed routes, one tick object shared by all subscribers.
* Bounded memory everywhere: strategy queues, the duplicate LRU, the limiter's 10-entry deque.
* CSV flushes are batched; fsync only at shutdown (a per-row fsync would cap throughput; a crash can lose at most the rows since the last flush, at most one minute's candles).
* Scaling beyond one core (not needed at these rates): shard instruments across processes, each with its own aggregator and file; the executor's limit is global, so it stays a single dispatcher by design.

## Project layout

```
market_data/      models, client, publisher, candles, bus, strategy, csv_writer, app, __main__
order_executor/   models, rate_limiter, executor, exchange, __main__
tests/            test_market_data_units.py, test_market_data_e2e.py, test_order_executor.py
requirements.txt  pyproject.toml
```
