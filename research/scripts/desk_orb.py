"""Executable opening-range continuation rule for the desk.

This is a paper-execution variant of the frozen ORB screen, not the screen
itself. It keeps the signal geometry (09:30-10:00 range, trailing-median width
filter, first close beyond the range from 10:01 to 14:00, next available bar,
one entry per session, earliest trigger and symbol-order tie-break) and uses
IEX bars and quotes. The charter adds a 50 bp stop and the 15:50 flatten.

The rule is long-only. The desk config has allow_short=false, and a short leg
would be a separate, explicitly approved change.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Mapping
from zoneinfo import ZoneInfo

from desk_broker import MinuteBar
from desk_config import SYMBOLS

ET = ZoneInfo("America/New_York")
OR_START_MINUTE = 9 * 60 + 30
OR_END_MINUTE = 10 * 60
TRIGGER_START_MINUTE = 10 * 60 + 1
TRIGGER_END_MINUTE = 14 * 60
LAST_ENTRY_MINUTE = 14 * 60 + 1
HISTORY_SESSIONS = 20
HISTORY_LOOKBACK_DAYS = 45

ORB_PARAMETERS: dict[str, object] = {
    "rule": "orb_iex_long_v1",
    "feed": "iex",
    "opening_range_start_et": "09:30",
    "opening_range_end_et": "10:00",
    "trigger_start_et": "10:01",
    "trigger_end_et": "14:00",
    "last_entry_et": "14:01",
    "history_sessions": HISTORY_SESSIONS,
    "width_filter": "trailing_median",
    "direction": "long",
    "one_entry_per_session": True,
    "stop_bps": 50,
    "exit_et": "15:50",
}


@dataclass(frozen=True)
class OpeningRange:
    symbol: str
    high: float
    low: float
    width_ratio: float


@dataclass(frozen=True)
class Trigger:
    symbol: str
    direction: str
    trigger_minute: int
    trigger_close: float
    range_high: float
    range_low: float
    entry_minute: int


def minute_of(stamp: datetime) -> int:
    local = stamp.astimezone(ET)
    return local.hour * 60 + local.minute


def session_date_of(stamp: datetime) -> str:
    return stamp.astimezone(ET).date().isoformat()


def validate_parameters(parameters: Mapping[str, object]) -> None:
    if dict(parameters) != ORB_PARAMETERS:
        raise ValueError("candidate parameters do not match the predeclared ORB rule")


def _indexed(bars: Iterable[MinuteBar]) -> dict[int, MinuteBar]:
    return {minute_of(bar.start): bar for bar in bars}


def opening_ranges(bars_by_symbol: Mapping[str, Iterable[MinuteBar]], *,
                   symbols: Iterable[str] = SYMBOLS) -> dict[str, OpeningRange]:
    """Session opening range per symbol; a missing minute drops the symbol."""
    ranges: dict[str, OpeningRange] = {}
    for symbol in symbols:
        bars = bars_by_symbol.get(symbol)
        if not bars:
            continue
        indexed = _indexed(bars)
        window = [indexed.get(minute) for minute in range(OR_START_MINUTE, OR_END_MINUTE + 1)]
        if any(bar is None for bar in window):
            continue
        high = max(bar.high for bar in window)
        low = min(bar.low for bar in window)
        if high <= low:
            continue
        mid = (high + low) / 2.0
        ranges[symbol] = OpeningRange(symbol, high, low, (high - low) / mid)
    return ranges


def prior_session_ratios(bars_by_symbol: Mapping[str, Iterable[MinuteBar]],
                         session_date: str, *, symbols: Iterable[str] = SYMBOLS) -> dict[str, list[float]]:
    """Per-symbol OR width ratios for complete sessions strictly before session_date."""
    symbols = tuple(symbols)
    by_symbol_date: dict[str, dict[str, list[MinuteBar]]] = {symbol: {} for symbol in symbols}
    for symbol in symbols:
        for bar in bars_by_symbol.get(symbol, ()):
            day = session_date_of(bar.start)
            if day >= session_date:
                continue
            by_symbol_date[symbol].setdefault(day, []).append(bar)
    ratios: dict[str, list[float]] = {symbol: [] for symbol in symbols}
    for symbol in symbols:
        for day in sorted(by_symbol_date[symbol]):
            found = opening_ranges({symbol: by_symbol_date[symbol][day]}, symbols=(symbol,)).get(symbol)
            if found is not None:
                ratios[symbol].append(found.width_ratio)
    return ratios


def median_ratios(ratios: Mapping[str, list[float]],
                  sessions: int = HISTORY_SESSIONS, *, symbols: Iterable[str] = SYMBOLS) -> dict[str, float]:
    result: dict[str, float] = {}
    for symbol in symbols:
        history = ratios.get(symbol, [])
        if len(history) >= sessions:
            result[symbol] = statistics.median(history[-sessions:])
    return result


def eligible_symbols(ranges: Mapping[str, OpeningRange],
                     medians: Mapping[str, float]) -> list[str]:
    return [symbol for symbol in SYMBOLS
            if symbol in ranges and symbol in medians
            and ranges[symbol].width_ratio >= medians[symbol]]


def _next_bar_minute(indexed: Mapping[int, MinuteBar], trigger_minute: int) -> int | None:
    for minute in range(trigger_minute + 1, LAST_ENTRY_MINUTE + 1):
        if minute in indexed:
            return minute
    return None


def select_trigger(bars_by_symbol: Mapping[str, Iterable[MinuteBar]],
                   ranges: Mapping[str, OpeningRange],
                   medians: Mapping[str, float], *, now_minute: int,
                   direction: str = "long") -> Trigger | None:
    """Earliest qualifying close; ties by the fixed symbol order.

    Only bars that have closed by now_minute are considered: the bar starting
    at now_minute - 1 is the latest final bar.
    """
    if direction not in {"long", "short"}:
        raise ValueError("direction must be long or short")
    eligible = eligible_symbols(ranges, medians)
    if not eligible:
        return None
    indexed = {symbol: _indexed(bars_by_symbol.get(symbol, ())) for symbol in eligible}
    last_final = min(TRIGGER_END_MINUTE, now_minute - 1)
    for minute in range(TRIGGER_START_MINUTE, last_final + 1):
        for symbol in eligible:
            bar = indexed[symbol].get(minute)
            if bar is None:
                continue
            opening = ranges[symbol]
            beyond = bar.close > opening.high if direction == "long" else bar.close < opening.low
            if not beyond:
                continue
            entry = _next_bar_minute(indexed[symbol], minute)
            if entry is None:
                continue
            return Trigger(symbol, direction, minute, bar.close,
                           opening.high, opening.low, entry)
    return None


def history_window(session_date: date) -> tuple[datetime, datetime]:
    """Calendar window used to fetch enough prior sessions for the median."""
    start = datetime(session_date.year, session_date.month, session_date.day,
                     OR_START_MINUTE // 60, OR_START_MINUTE % 60, tzinfo=ET)
    return start - timedelta(days=HISTORY_LOOKBACK_DAYS), start
