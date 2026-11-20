"""Tick model, message decoding and validation.

All timestamps inside the module are integers: nanoseconds since the Unix
epoch, UTC, taken from the exchange's ``timestamp`` field (never the local
clock). Integers keep minute bucketing exact (no float rounding at the
boundary) and comparisons cheap.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

NS_PER_US = 1_000
NS_PER_MS = 1_000_000
NS_PER_SECOND = 1_000_000_000
NS_PER_MINUTE = 60 * NS_PER_SECOND

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MAX_INSTRUMENT_LEN = 32


class TickValidationError(ValueError):
    """A message that is not a valid tick (bad JSON, missing / wrong field)."""


@dataclass(frozen=True, slots=True)
class Tick:
    """One executed trade. Immutable, so one instance is shared by every
    subscriber (no per-strategy copies)."""

    instrument: str
    ts_ns: int  # exchange timestamp, ns since epoch (UTC)
    ltp: float  # last traded price
    ltq: int  # last traded quantity

    @property
    def timestamp(self) -> str:
        return format_ts(self.ts_ns)


def parse_ts(value: Any) -> int:
    """ISO-8601 exchange timestamp -> ns since epoch. A timezone is required
    (``Z`` or an offset): a naive timestamp is ambiguous and rejected."""
    if not isinstance(value, str) or not value:
        raise TickValidationError("timestamp must be a non-empty ISO-8601 string")
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise TickValidationError(f"bad timestamp {value!r}") from exc
    if dt.tzinfo is None:
        raise TickValidationError(f"timestamp {value!r} has no timezone")
    delta = dt - _EPOCH  # exact integer arithmetic, no float seconds
    return (delta.days * 86_400 + delta.seconds) * NS_PER_SECOND + delta.microseconds * NS_PER_US


def format_ts(ts_ns: int) -> str:
    """ns since epoch -> ISO-8601 UTC with milliseconds, e.g. 2026-11-17T09:15:00.125Z."""
    secs, rem = divmod(ts_ns, NS_PER_SECOND)
    dt = datetime.fromtimestamp(secs, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{rem // NS_PER_MS:03d}Z"


def parse_tick(obj: Any) -> Tick:
    """Validate one decoded message and build a Tick."""
    if not isinstance(obj, dict):
        raise TickValidationError("tick must be a JSON object")
    instrument = obj.get("instrument")
    if not isinstance(instrument, str) or not instrument.strip():
        raise TickValidationError("instrument must be a non-empty string")
    instrument = instrument.strip()
    if len(instrument) > MAX_INSTRUMENT_LEN:
        raise TickValidationError("instrument name too long")

    ltp = obj.get("ltp")
    if isinstance(ltp, bool) or not isinstance(ltp, (int, float)):
        raise TickValidationError("ltp must be a number")
    ltp = float(ltp)
    if not math.isfinite(ltp) or ltp <= 0:
        raise TickValidationError("ltp must be a positive finite number")

    ltq = obj.get("ltq")
    if isinstance(ltq, bool) or not isinstance(ltq, (int, float)):
        raise TickValidationError("ltq must be a number")
    if isinstance(ltq, float):
        if not ltq.is_integer():
            raise TickValidationError("ltq must be a whole quantity")
        ltq = int(ltq)
    if ltq <= 0:
        raise TickValidationError("ltq must be positive")

    return Tick(instrument=instrument, ts_ns=parse_ts(obj.get("timestamp")), ltp=ltp, ltq=ltq)


def decode_message(raw: str | bytes) -> list[Any]:
    """One WebSocket frame -> a list of decoded objects. A frame may hold one
    tick object or a JSON array of ticks (batching)."""
    try:
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise TickValidationError("frame is not valid JSON") from exc
    if isinstance(data, list):
        return data
    return [data]
