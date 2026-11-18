"""Deterministic, data-only IEX minute-bar accumulation for one symbol.

Native IEX bars take precedence over quote-mid fallback bars. Quote-mid bars
carry zero volume and must not be used to calculate trade-volume VWAP.
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return stamp.astimezone(timezone.utc)


def _number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise ValueError("bar value must be finite")
    return float(value)


class SessionBars:
    """Accumulate one session and restore it from an atomic snapshot on restart."""

    def __init__(self, symbol: str, session_date: str):
        self.symbol = symbol
        self.session_date = session_date
        self.bars: dict[str, dict[str, Any]] = {}
        self.observed_at: datetime | None = None

    @classmethod
    def load(cls, path: Path, symbol: str, session_date: str) -> "SessionBars":
        with path.open("rb") as stream:
            raw = stream.read(1_000_001)
        if len(raw) > 1_000_000:
            raise ValueError("session bar snapshot exceeds size limit")
        payload = json.loads(raw)
        if (not isinstance(payload, dict) or payload.get("version") != 1
                or payload.get("symbol") != symbol or payload.get("session_date") != session_date
                or not isinstance(payload.get("bars"), list) or len(payload["bars"]) > 390):
            raise ValueError("invalid session bar snapshot")
        observed = _timestamp(payload.get("observed_at"))
        book = cls(symbol, session_date)
        book.observed_at = observed
        for item in payload["bars"]:
            if not isinstance(item, dict):
                raise ValueError("invalid bar")
            stamp = _timestamp(item.get("time"))
            key = book._minute(stamp)
            if key in book.bars or stamp > observed or stamp.second or stamp.microsecond:
                raise ValueError("invalid or duplicate minute")
            values = [_number(item.get(k)) for k in ("open", "high", "low", "close", "volume")]
            opening, high, low, close, volume = values
            if min(opening, high, low, close) <= 0 or volume < 0 or high < max(opening, close) or low > min(opening, close):
                raise ValueError("invalid OHLCV")
            source = item.get("source")
            if source == "quote_mid":
                first, last = _timestamp(item.get("first_quote_time")), _timestamp(item.get("last_quote_time"))
                if volume != 0 or first > last or book._minute(first) != key or book._minute(last) != key or last > observed:
                    raise ValueError("invalid quote bar")
            elif source != "iex" or stamp + timedelta(minutes=1) > observed:
                raise ValueError("invalid native bar")
            if source == "iex" and item.get("vwap") is not None and _number(item["vwap"]) <= 0:
                raise ValueError("invalid IEX VWAP")
            book.bars[key] = dict(item)
        return book

    def _minute(self, stamp: datetime) -> str:
        local = stamp.astimezone(ET)
        if local.date().isoformat() != self.session_date or not time(9, 30) <= local.time() < time(16):
            raise ValueError("timestamp outside regular session")
        return stamp.replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")

    def add_bar(self, row: Mapping[str, Any], received_at: datetime) -> None:
        if row.get("S") != self.symbol or row.get("T") not in ("b", "u"):
            raise ValueError("unexpected bar symbol or type")
        stamp = _timestamp(row.get("t"))
        if stamp.second or stamp.microsecond or stamp + timedelta(minutes=1) > received_at:
            raise ValueError("bar is not a completed minute")
        key = self._minute(stamp)
        opening, high, low, close, volume = (_number(row.get(k)) for k in ("o", "h", "l", "c", "v"))
        if min(opening, high, low, close) <= 0 or volume < 0 or high < max(opening, close) or low > min(opening, close):
            raise ValueError("invalid OHLCV")
        vwap = _number(row["vw"]) if row.get("vw") is not None else None
        if vwap is not None and vwap <= 0:
            raise ValueError("invalid IEX VWAP")
        self.bars[key] = {"time": key, "open": opening, "high": high, "low": low,
                          "close": close, "volume": volume, "vwap": vwap, "source": "iex"}

    def add_quote(self, quote: Mapping[str, Any], received_at: datetime) -> None:
        if quote.get("symbol") != self.symbol:
            raise ValueError("unexpected quote symbol")
        stamp = _timestamp(quote.get("source_timestamp_utc"))
        if stamp > received_at:
            raise ValueError("quote from the future")
        key = self._minute(stamp)
        bid, ask = _number(quote.get("bid")), _number(quote.get("ask"))
        if bid <= 0 or ask < bid:
            raise ValueError("invalid quote")
        midpoint = (bid + ask) / 2
        existing = self.bars.get(key)
        if existing and existing["source"] == "iex":
            return
        if existing is None:
            self.bars[key] = {"time": key, "open": midpoint, "high": midpoint, "low": midpoint,
                              "close": midpoint, "volume": 0.0, "source": "quote_mid",
                              "first_quote_time": stamp.isoformat(), "last_quote_time": stamp.isoformat()}
            return
        existing["high"] = max(existing["high"], midpoint)
        existing["low"] = min(existing["low"], midpoint)
        first_stamp = _timestamp(existing["first_quote_time"])
        last_stamp = _timestamp(existing["last_quote_time"])
        if stamp < first_stamp or (stamp == first_stamp and midpoint < existing["open"]):
            existing["open"] = midpoint
            existing["first_quote_time"] = stamp.isoformat()
        if stamp > last_stamp or (stamp == last_stamp and midpoint > existing["close"]):
            existing["close"] = midpoint
            existing["last_quote_time"] = stamp.isoformat()

    def snapshot(self, observed_at: datetime) -> dict[str, Any]:
        if observed_at.tzinfo is None:
            raise ValueError("observation time must include a timezone")
        return {"version": 1, "symbol": self.symbol, "session_date": self.session_date,
                "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
                "bars": [dict(self.bars[key]) for key in sorted(self.bars)]}

    def save(self, path: Path, observed_at: datetime) -> None:
        payload = self.snapshot(observed_at)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".tmp")
        temporary.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False), encoding="utf-8")
        os.replace(temporary, path)
