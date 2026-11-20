"""Trade Order Executor: every exchange-bound action goes through ONE queue
and ONE dispatcher task that owns the rate limiter.

Concurrency model
-----------------
* Producers (asyncio tasks via ``submit`` / ``place`` / ``modify`` / ``cancel``,
  or OS threads via ``submit_threadsafe``) never touch the limiter or the
  exchange: they enqueue an action and get a future. Threads hand over with
  ``loop.call_soon_threadsafe``, so all queue state is mutated on the event
  loop thread only -- no locks, no check-then-act races. Producers never
  block on the limit; they await (or ignore) their future.
* A single dispatcher serialises dispatches, so two producers can never both
  take the 10th slot: the "11th action in the window" cannot happen by
  construction, not by locking discipline.
* The dispatcher never polls: it sleeps exactly until the next free slot (or
  until work arrives) and re-checks the limiter with the real clock after
  waking (``asyncio.sleep`` may fire up to one clock tick early).
* FIFO across producers: actions are dispatched in submission order, which
  also preserves causality per order (NEW before its MODIFY before its CANCEL).

Scheduling optimisations (``coalesce=True``, default) -- they never reorder an
order's actions and only ever save slots:
* MODIFY of an order whose NEW is still queued: merged into the NEW (one slot).
* MODIFY of an order with another MODIFY still queued: the queued one takes
  the newer price / quantity in place (keeps its queue position); the caller
  of the older modify gets ``SUPERSEDED``.
* CANCEL of an order whose NEW is still queued: neither is sent
  (``CANCELLED_BEFORE_SEND``) -- zero slots, and the order never reaches the market.
* CANCEL with modifies queued for the same order: the modifies are dropped
  (``SUPERSEDED``); only the cancel is sent.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import itertools
import logging
import time
from collections import deque
from dataclasses import dataclass, field

from .exchange import SimulatedExchange
from .models import ActionResult, ActionType, OrderAction, Side, Status, cancel_order, modify_order, new_order
from .rate_limiter import SlidingWindowRateLimiter

log = logging.getLogger(__name__)


@dataclass(eq=False)
class _Pending:
    action: OrderAction  # what will be sent (may absorb later modifies)
    enqueued_ns: int
    waiters: list[tuple[asyncio.Future[ActionResult], OrderAction, Status]] = field(default_factory=list)
    alive: bool = True


@dataclass
class ExecutorStats:
    submitted: int = 0
    dispatched: int = 0
    saved_by_coalescing: int = 0
    rejected: int = 0
    max_queue: int = 0


class TradeOrderExecutor:
    def __init__(
        self,
        exchange: SimulatedExchange,
        limiter: SlidingWindowRateLimiter | None = None,
        coalesce: bool = True,
    ) -> None:
        self.exchange = exchange
        self.limiter = limiter or SlidingWindowRateLimiter()
        self.coalesce = coalesce
        self._queue: deque[_Pending] = deque()
        self._by_order: dict[str, list[_Pending]] = {}  # queued (alive) actions per order id
        self._orders: dict[str, str] = {}  # order id -> "queued" | "live" | "cancelling"
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._accepting = False
        self._ids = itertools.count(1)
        self.stats = ExecutorStats()

    # ---------------------------------------------------------------- lifecycle
    async def start(self) -> "TradeOrderExecutor":
        self._loop = asyncio.get_running_loop()
        self._accepting = True
        self._task = asyncio.create_task(self._dispatch_loop(), name="order-dispatcher")
        return self

    async def stop(self, drain: bool = True, timeout: float | None = None) -> None:
        """Stop accepting. ``drain=True`` dispatches everything already queued
        (at the rate limit) before closing; ``drain=False`` resolves queued
        actions as SHUTDOWN without sending them."""
        self._accepting = False
        if drain:
            try:
                await asyncio.wait_for(self._drained(), timeout)
            except TimeoutError:
                log.warning("drain timed out; %d action(s) not sent", self.pending)
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        while self._queue:
            p = self._queue.popleft()
            if p.alive:
                self._resolve_all(p, Status.SHUTDOWN, detail="executor stopped before dispatch")
        self._by_order.clear()
        self.exchange.close()

    async def _drained(self) -> None:
        while any(p.alive for p in self._queue):
            await asyncio.sleep(0.01)

    @property
    def pending(self) -> int:
        return sum(1 for p in self._queue if p.alive)

    # ---------------------------------------------------------------- producer API
    def new_order_id(self) -> str:
        return f"O{next(self._ids):06d}"

    def submit(self, action: OrderAction) -> asyncio.Future[ActionResult]:
        """Event-loop thread only. Returns at once; the future resolves when the
        action is dispatched (or coalesced / rejected)."""
        loop = self._loop
        if loop is None:
            raise RuntimeError("executor not started")
        fut: asyncio.Future[ActionResult] = loop.create_future()
        self._enqueue(action, fut)
        return fut

    def submit_threadsafe(self, action: OrderAction) -> concurrent.futures.Future[ActionResult]:
        """Any thread. Returns a concurrent.futures.Future."""
        loop = self._loop
        if loop is None:
            raise RuntimeError("executor not started")
        cf: concurrent.futures.Future[ActionResult] = concurrent.futures.Future()

        def enqueue() -> None:
            fut = loop.create_future()
            fut.add_done_callback(lambda f: cf.set_exception(f.exception()) if f.exception() else cf.set_result(f.result()))
            self._enqueue(action, fut)

        loop.call_soon_threadsafe(enqueue)
        return cf

    async def place(self, instrument: str, side: Side | str, quantity: int, price: float,
                    order_id: str | None = None, producer: str = "") -> ActionResult:
        return await self.submit(new_order(order_id or self.new_order_id(), instrument, side, quantity, price, producer))

    async def modify(self, order_id: str, quantity: int | None = None, price: float | None = None,
                     producer: str = "") -> ActionResult:
        return await self.submit(modify_order(order_id, quantity, price, producer))

    async def cancel(self, order_id: str, producer: str = "") -> ActionResult:
        return await self.submit(cancel_order(order_id, producer))

    # ---------------------------------------------------------------- queueing (loop thread)
    def _enqueue(self, action: OrderAction, fut: asyncio.Future[ActionResult]) -> None:
        self.stats.submitted += 1
        now = time.monotonic_ns()
        if not self._accepting:
            return self._reject(fut, action, "executor is not accepting actions")
        oid = action.order_id
        state = self._orders.get(oid)
        queued = [p for p in self._by_order.get(oid, ()) if p.alive]

        if action.type is ActionType.NEW:
            if state is not None:
                return self._reject(fut, action, "duplicate order id")
            self._orders[oid] = "queued"
            return self._append(_Pending(action, now, [(fut, action, Status.DISPATCHED)]))

        if state is None:
            return self._reject(fut, action, "unknown order id")
        if state == "cancelling":
            return self._reject(fut, action, "order is already being cancelled")

        if action.type is ActionType.MODIFY:
            if self.coalesce and queued:
                last = queued[-1]
                if last.action.type is ActionType.NEW:  # merge into the queued NEW
                    last.action = last.action.merged(action)
                    last.waiters.append((fut, action, Status.COALESCED))
                    self.stats.saved_by_coalescing += 1
                    return None
                if last.action.type is ActionType.MODIFY:  # newer values win, same queue slot
                    last.action = last.action.merged(action)
                    for wf, wa, _ in last.waiters:
                        self._set(wf, ActionResult(Status.SUPERSEDED, wa, detail="replaced by a newer modify before it was sent"))
                    last.waiters = [(fut, action, Status.DISPATCHED)]
                    self.stats.saved_by_coalescing += 1
                    return None
            return self._append(_Pending(action, now, [(fut, action, Status.DISPATCHED)]))

        # CANCEL
        self._orders[oid] = "cancelling"
        if self.coalesce and queued:
            if queued[0].action.type is ActionType.NEW:  # never sent: drop NEW, its modifies and the cancel
                for p in queued:
                    self._resolve_all(p, Status.CANCELLED_BEFORE_SEND, detail="cancelled while queued; nothing was sent")
                self.stats.saved_by_coalescing += len(queued) + 1
                self._orders.pop(oid, None)
                self._set(fut, ActionResult(Status.CANCELLED_BEFORE_SEND, action, detail="order was still queued; nothing sent"))
                return None
            for p in queued:  # pending modifies are pointless now
                self._resolve_all(p, Status.SUPERSEDED, detail="order cancelled before this modify was sent")
                self.stats.saved_by_coalescing += 1
        return self._append(_Pending(action, now, [(fut, action, Status.DISPATCHED)]))

    def _append(self, p: _Pending) -> None:
        self._queue.append(p)
        self._by_order.setdefault(p.action.order_id, []).append(p)
        self.stats.max_queue = max(self.stats.max_queue, len(self._queue))
        self._wake.set()

    def _reject(self, fut: asyncio.Future[ActionResult], action: OrderAction, why: str) -> None:
        self.stats.rejected += 1
        self._set(fut, ActionResult(Status.REJECTED, action, detail=why))

    @staticmethod
    def _set(fut: asyncio.Future[ActionResult], res: ActionResult) -> None:
        if not fut.done():
            fut.set_result(res)

    def _resolve_all(self, p: _Pending, status: Status, detail: str = "") -> None:
        p.alive = False
        for wf, wa, _ in p.waiters:
            self._set(wf, ActionResult(status, wa, detail=detail))
        self._forget(p)

    def _forget(self, p: _Pending) -> None:
        lst = self._by_order.get(p.action.order_id)
        if lst is not None:
            if p in lst:
                lst.remove(p)
            if not lst:
                del self._by_order[p.action.order_id]

    # ---------------------------------------------------------------- dispatcher
    async def _dispatch_loop(self) -> None:
        lim = self.limiter
        while True:
            while self._queue and not self._queue[0].alive:
                self._queue.popleft()
            if not self._queue:
                self._wake.clear()
                await self._wake.wait()
                continue
            now = lim.clock_ns()
            slot = lim.next_slot_ns(now)
            if slot > now:
                # Sleep until the slot frees. The head may be coalesced or
                # cancelled meanwhile, so it is only taken after waking.
                await asyncio.sleep((slot - now) / 1e9)
                continue
            p = self._queue.popleft()
            self._forget(p)
            p.alive = False
            dispatch_ns = lim.clock_ns()
            try:
                ack = self.exchange.dispatch(p.action, dispatch_ns)
            except Exception as exc:  # exchange failure: the action was not recorded
                lim.record(lim.clock_ns())  # conservatively count the attempt
                for wf, wa, _ in p.waiters:
                    self._set(wf, ActionResult(Status.REJECTED, wa, detail=f"exchange error: {exc}"))
                continue
            lim.record(lim.clock_ns())  # AFTER the call: upper bound of the exchange's stamp
            self.stats.dispatched += 1
            self._after_dispatch(p, ack.accepted)
            wait_s = (dispatch_ns - p.enqueued_ns) / 1e9
            for wf, wa, st in p.waiters:
                status = st if ack.accepted else Status.REJECTED
                self._set(wf, ActionResult(status, wa, sent=p.action, dispatch_mono_ns=dispatch_ns,
                                           exchange_seq=ack.seq, queue_wait_s=wait_s,
                                           detail=ack.reason))

    def _after_dispatch(self, p: _Pending, accepted: bool) -> None:
        oid, t = p.action.order_id, p.action.type
        if t is ActionType.NEW:
            if accepted:
                self._orders[oid] = "live"
            else:
                self._orders.pop(oid, None)
        elif t is ActionType.CANCEL:
            self._orders.pop(oid, None)
