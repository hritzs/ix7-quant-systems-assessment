"""Order actions and results."""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum


class ActionType(str, Enum):
    NEW = "NEW"
    MODIFY = "MODIFY"
    CANCEL = "CANCEL"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class Status(str, Enum):
    DISPATCHED = "DISPATCHED"  # sent to the exchange (counted against the limit)
    COALESCED = "COALESCED"  # merged into a pending action that was then dispatched (no extra slot)
    SUPERSEDED = "SUPERSEDED"  # replaced by a newer modify / a cancel before it was sent (never sent)
    CANCELLED_BEFORE_SEND = "CANCELLED_BEFORE_SEND"  # NEW cancelled while still queued: neither is sent
    REJECTED = "REJECTED"  # refused locally (unknown order, duplicate id, executor stopped)
    SHUTDOWN = "SHUTDOWN"  # still queued when the executor stopped without draining


@dataclass(frozen=True, slots=True)
class OrderAction:
    type: ActionType
    order_id: str
    instrument: str | None = None
    side: Side | None = None
    quantity: int | None = None
    price: float | None = None
    producer: str = ""

    def merged(self, newer: "OrderAction") -> "OrderAction":
        """This action with the price / quantity of a newer MODIFY applied."""
        return replace(
            self,
            quantity=newer.quantity if newer.quantity is not None else self.quantity,
            price=newer.price if newer.price is not None else self.price,
        )


def new_order(order_id: str, instrument: str, side: Side | str, quantity: int, price: float, producer: str = "") -> OrderAction:
    if quantity <= 0 or price <= 0:
        raise ValueError("quantity and price must be positive")
    return OrderAction(ActionType.NEW, order_id, instrument, Side(side), quantity, price, producer)


def modify_order(order_id: str, quantity: int | None = None, price: float | None = None, producer: str = "") -> OrderAction:
    if quantity is None and price is None:
        raise ValueError("a modification needs a new quantity or price")
    return OrderAction(ActionType.MODIFY, order_id, quantity=quantity, price=price, producer=producer)


def cancel_order(order_id: str, producer: str = "") -> OrderAction:
    return OrderAction(ActionType.CANCEL, order_id, producer=producer)


@dataclass(frozen=True, slots=True)
class ActionResult:
    status: Status
    action: OrderAction  # what the caller asked for
    sent: OrderAction | None = None  # what actually went to the exchange (after coalescing)
    dispatch_mono_ns: int | None = None
    exchange_seq: int | None = None
    queue_wait_s: float = 0.0
    detail: str = ""
