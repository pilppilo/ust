"""Input quality judge (build item C1).

Pure and offline: raw inputs in, a recorded verdict per input out. It reads no
file, no broker and no clock (``now`` is passed in), places nothing and imports
nothing from the engine. A strategy never sees an input the judge called
``unusable``; every verdict carries reason codes and the thresholds that produced
it, so a later audit can say exactly why a tick was used or refused.

Inputs judged on each tick
  feed       the quote-stream snapshot flags (connected, authenticated, subscribed,
             fresh) and the outage streak
  quote      structure, age, crossed/locked, spread, source tag, conflicting prices
             at one timestamp, out-of-order time, implausible jumps
  reference  the prior quote a momentum rule compares against (same run, same
             connection epoch, same Eastern date, 0 < age <= max gap)
  calendar   weekday, regular session, published holidays and early closes; the
             broker clock overrides the table when the caller supplies it

Verdicts: ``usable`` < ``degraded`` < ``unusable``. ``gate`` turns verdicts into
an entry decision: an entry needs every *required* input usable. An exit or a
protection check is never blocked by the judge (staleness blocks entries, never
exits); problems are recorded as advisory so the record still shows them.

What this does not do: it does not judge strategy value, news, or consolidated
versus IEX agreement (S4). IEX is tagged and, unless a protocol allows it, an
unknown source is refused. Limits of the replay inputs are in the C1 RESULT.
"""
from __future__ import annotations

import hashlib
import json
import math
from collections import OrderedDict
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

SCHEMA = "input-quality-v1"
ET = ZoneInfo("America/New_York")
USABLE, DEGRADED, UNUSABLE = "usable", "degraded", "unusable"
_RANK = {USABLE: 0, DEGRADED: 1, UNUSABLE: 2}

# NYSE 2026 schedule (holidays and the two 13:00 early closes), checked 2026-11-12 against a
# web-search summary of nyse.com/markets/hours-calendars (not a direct read of the PDF). This
# table is a fallback and the broker clock wins when supplied. ICE's futures calendar lists
# other early-close dates that are not NYSE equities dates. 2027 is NOT in the table. A date outside the
# table's year is "calendar_unverified" (degraded), never assumed to be a normal day.
CALENDAR_YEARS = (2026,)
HOLIDAYS = frozenset(date.fromisoformat(d) for d in (
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03", "2026-05-25",
    "2026-06-19", "2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25"))
EARLY_CLOSES = {date.fromisoformat("2026-11-27"): time(13, 0),
                date.fromisoformat("2026-12-24"): time(13, 0)}
OPEN, CLOSE = time(9, 30), time(16, 0)


@dataclass(frozen=True)
class QualityConfig:
    max_quote_age_s: float = 5.0          # older than this: unusable
    degraded_quote_age_s: float = 2.0     # older than this: degraded
    future_tolerance_s: float = 1.0       # quote stamped this far ahead of now: clock skew
    degraded_spread_bps: float = 10.0
    unusable_spread_bps: float = 50.0     # implausible for the liquid names in scope
    max_jump_bps: float = 100.0           # mid move between consecutive quotes
    reference_max_gap_s: float = 60.0
    outage_streak_ticks: int = 10
    allowed_sources: tuple[str, ...] = ("iex",)
    require_epoch: bool = True            # replay of rows without an epoch sets False
    seen_timestamps: int = 64

    def digest(self) -> str:
        blob = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()[:16]


def _verdict(name: str, reasons: list[tuple[str, str]], details: dict[str, Any]) -> dict[str, Any]:
    """reasons: (severity, code). The verdict is the worst severity present."""
    worst = max((s for s, _ in reasons), key=_RANK.__getitem__, default=USABLE)
    return {"input": name, "verdict": worst, "reasons": [c for _, c in reasons], "details": details}


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp.astimezone(timezone.utc) if stamp.tzinfo is not None else None


class QualityState:
    """What the judge remembers between ticks. Replaying the same ticks from a
    fresh state gives the same verdicts."""

    def __init__(self, config: QualityConfig | None = None):
        self.config = config or QualityConfig()
        self.prior: dict[str, Any] | None = None       # last accepted quote
        self.seen: OrderedDict[str, tuple[float, float]] = OrderedDict()
        self.max_ts: dict[Any, datetime] = {}           # per connection epoch
        self.outage_streak = 0

    # One-shot runners (a timer invokes the CLI every cycle) keep this between processes.
    def dump(self) -> dict[str, Any]:
        prior = self.prior
        return {"schema": SCHEMA, "config_digest": self.config.digest(),
                "prior": None if prior is None else {
                    "stamp": prior["stamp"].isoformat(), "mid": prior["mid"],
                    "epoch": prior["epoch"], "run_id": prior["run_id"]},
                "seen": [[k, v[0], v[1]] for k, v in self.seen.items()],
                "max_ts": [[epoch, ts.isoformat()] for epoch, ts in self.max_ts.items()],
                "outage_streak": self.outage_streak}

    @classmethod
    def load(cls, data: Any, config: QualityConfig | None = None) -> "QualityState":
        """Restore from ``dump``. Anything unreadable, or written under other
        thresholds, gives a fresh state: the reference warms up again, which only
        ever makes the judge more cautious."""
        state = cls(config)
        try:
            if (not isinstance(data, dict) or data.get("schema") != SCHEMA
                    or data.get("config_digest") != state.config.digest()):
                return state
            prior = data.get("prior")
            if prior is not None:
                stamp = _parse_ts(prior["stamp"])
                mid = _num(prior["mid"])
                if stamp is None or mid is None:
                    return cls(config)
                state.prior = {"stamp": stamp, "mid": mid, "epoch": prior["epoch"],
                               "run_id": prior["run_id"]}
            for key, bid, ask in data.get("seen") or []:
                state.seen[str(key)] = (float(bid), float(ask))
            for epoch, ts in data.get("max_ts") or []:
                parsed = _parse_ts(ts)
                if parsed is None:
                    return cls(config)
                state.max_ts[epoch] = parsed
            state.outage_streak = max(0, int(data.get("outage_streak") or 0))
        except (KeyError, TypeError, ValueError):
            return cls(config)
        return state


def judge_calendar(now: datetime, broker_clock: dict[str, Any] | None = None) -> dict[str, Any]:
    local = now.astimezone(ET)
    day, clock = local.date(), local.time()
    reasons: list[tuple[str, str]] = []
    details: dict[str, Any] = {"eastern_date": day.isoformat(), "eastern_time": clock.isoformat(timespec="seconds")}
    table_open = None
    if day.weekday() >= 5:
        table_open, why = False, "weekend"
    elif day in HOLIDAYS:
        table_open, why = False, "exchange_holiday"
    else:
        close = EARLY_CLOSES.get(day, CLOSE)
        details["table_close"] = close.isoformat(timespec="minutes")
        table_open, why = OPEN <= clock < close, "outside_regular_hours"
        if day in EARLY_CLOSES:
            details["early_close"] = True
    covered = day.year in CALENDAR_YEARS
    if broker_clock is not None:
        is_open = broker_clock.get("is_open")
        details["authority"] = "broker_clock"
        if is_open is not True:
            reasons.append((UNUSABLE, "broker_clock_closed"))
        if covered and bool(is_open) != bool(table_open):
            reasons.append((DEGRADED, "calendar_table_disagrees_with_broker_clock"))
    else:
        details["authority"] = "table" if covered else "none"
        if not table_open:
            reasons.append((UNUSABLE, why))
        elif not covered:
            reasons.append((DEGRADED, "calendar_unverified"))
    return _verdict("calendar", reasons, details)


def judge_feed(feed: dict[str, Any] | None, state: QualityState) -> dict[str, Any]:
    cfg = state.config
    reasons: list[tuple[str, str]] = []
    details: dict[str, Any] = {}
    if not isinstance(feed, dict):
        state.outage_streak += 1
        reasons.append((UNUSABLE, "feed_unavailable"))
    else:
        for flag in ("connected", "authenticated", "subscribed"):
            if feed.get(flag) is not True:
                reasons.append((UNUSABLE, f"feed_{flag}_false" if flag in feed else f"feed_{flag}_unrecorded"))
        if "fresh" in feed and feed["fresh"] is not True:
            reasons.append((UNUSABLE, "feed_not_fresh"))
        elif "fresh" not in feed:
            reasons.append((UNUSABLE, "feed_fresh_unrecorded"))
        if feed.get("source_age_ms") is not None:
            details["source_age_ms"] = feed["source_age_ms"]
        if feed.get("receipt_age_ms") is not None:
            details["receipt_age_ms"] = feed["receipt_age_ms"]
        state.outage_streak = state.outage_streak + 1 if reasons else 0
    if state.outage_streak:
        details["outage_streak_ticks"] = state.outage_streak
        if state.outage_streak >= cfg.outage_streak_ticks:
            reasons.append((UNUSABLE, "feed_outage_streak"))
    return _verdict("feed", reasons, details)


def judge_quote(feed: dict[str, Any] | None, now: datetime, state: QualityState) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Returns (verdict, parsed quote or None). The parsed quote is what the
    reference check and the state use; it is never passed on if unusable."""
    cfg = state.config
    reasons: list[tuple[str, str]] = []
    details: dict[str, Any] = {}
    quote = feed.get("latest_quote") if isinstance(feed, dict) else None
    if not isinstance(quote, dict):
        return _verdict("quote", [(UNUSABLE, "quote_missing")], details), None
    stamp = _parse_ts(quote.get("timestamp"))
    bid, ask = _num(quote.get("bid")), _num(quote.get("ask"))
    if stamp is None:
        reasons.append((UNUSABLE, "quote_timestamp_invalid_or_naive"))
    if bid is None or ask is None:
        reasons.append((UNUSABLE, "quote_price_not_finite_number"))
    elif bid <= 0 or ask <= 0:
        reasons.append((UNUSABLE, "quote_price_not_positive"))
    elif bid > ask:
        reasons.append((UNUSABLE, "quote_crossed"))
    elif bid == ask:
        reasons.append((DEGRADED, "quote_locked"))
    source = str(quote.get("source") or (feed or {}).get("feed") or "").lower()
    details["source"] = source or None
    if source not in cfg.allowed_sources:
        reasons.append((UNUSABLE, "source_not_allowed"))
    epoch = quote.get("connection_epoch")
    if epoch is None and cfg.require_epoch:
        reasons.append((DEGRADED, "connection_epoch_unrecorded"))
    if any(s == UNUSABLE for s, _ in reasons) and (stamp is None or bid is None or ask is None):
        return _verdict("quote", reasons, details), None
    mid = spread_bps = None
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = (bid + ask) / 2
        spread_bps = (ask - bid) / mid * 10000
        details.update(bid=bid, ask=ask, mid=mid, spread_bps=round(spread_bps, 4))
        if spread_bps > cfg.unusable_spread_bps:
            reasons.append((UNUSABLE, "spread_implausible"))
        elif spread_bps > cfg.degraded_spread_bps:
            reasons.append((DEGRADED, "spread_wide"))
    if stamp is not None:
        age = (now - stamp).total_seconds()
        details["age_seconds"] = round(age, 6)
        if age < -cfg.future_tolerance_s:
            reasons.append((UNUSABLE, "quote_from_future"))
        elif age < 0:
            reasons.append((DEGRADED, "quote_clock_skew"))
        elif age > cfg.max_quote_age_s:
            reasons.append((UNUSABLE, "quote_stale"))
        elif age > cfg.degraded_quote_age_s:
            reasons.append((DEGRADED, "quote_aging"))
        key = stamp.isoformat()
        seen = state.seen.get(key)
        if seen is not None and bid is not None and ask is not None and seen != (bid, ask):
            reasons.append((UNUSABLE, "conflicting_quote_at_timestamp"))
            details["conflicts_with"] = {"bid": seen[0], "ask": seen[1]}
        elif seen is None and bid is not None and ask is not None:
            state.seen[key] = (bid, ask)
            while len(state.seen) > cfg.seen_timestamps:
                state.seen.popitem(last=False)
        last = state.max_ts.get(epoch)
        if last is not None and stamp < last:
            reasons.append((DEGRADED, "quote_out_of_order"))
        else:
            state.max_ts[epoch] = stamp
        prior = state.prior
        if (prior is not None and mid is not None and prior["epoch"] == epoch
                and 0 < (stamp - prior["stamp"]).total_seconds() <= cfg.reference_max_gap_s):
            jump = abs(mid / prior["mid"] - 1) * 10000
            if jump > cfg.max_jump_bps:
                reasons.append((DEGRADED, "implausible_jump"))
                details["jump_bps"] = round(jump, 4)
    verdict = _verdict("quote", reasons, details)
    parsed = None
    if verdict["verdict"] != UNUSABLE and stamp is not None and mid is not None:
        parsed = {"stamp": stamp, "mid": mid, "epoch": epoch, "run_id": (feed or {}).get("run_id")}
    return verdict, parsed


def judge_reference(parsed: dict[str, Any] | None, prior: dict[str, Any] | None,
                    cfg: QualityConfig) -> dict[str, Any]:
    if parsed is None:
        return _verdict("reference", [(UNUSABLE, "no_usable_quote_to_compare")], {})
    if prior is None:
        return _verdict("reference", [(UNUSABLE, "reference_warmup")], {})
    if prior["run_id"] != parsed["run_id"] or prior["epoch"] != parsed["epoch"]:
        return _verdict("reference", [(UNUSABLE, "reference_restart_or_reconnect")], {})
    elapsed = (parsed["stamp"] - prior["stamp"]).total_seconds()
    details = {"reference_age_seconds": round(elapsed, 6)}
    if parsed["stamp"].astimezone(ET).date() != prior["stamp"].astimezone(ET).date():
        return _verdict("reference", [(UNUSABLE, "reference_other_session")], details)
    if elapsed <= 0:
        return _verdict("reference", [(UNUSABLE, "reference_not_older_than_quote")], details)
    if elapsed > cfg.reference_max_gap_s:
        return _verdict("reference", [(UNUSABLE, "reference_gap")], details)
    return _verdict("reference", [], details)


def judge_tick(feed: dict[str, Any] | None, now: datetime, state: QualityState,
               broker_clock: dict[str, Any] | None = None) -> dict[str, Any]:
    """One tick. ``now`` must be timezone-aware. Mutates only ``state``."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)
    cfg = state.config
    calendar = judge_calendar(now, broker_clock)
    feed_v = judge_feed(feed, state)
    if feed_v["verdict"] == UNUSABLE:
        quote_v, parsed = _verdict("quote", [(UNUSABLE, "feed_unusable_quote_not_judged")], {}), None
    else:
        quote_v, parsed = judge_quote(feed, now, state)
    ref_v = judge_reference(parsed, state.prior, cfg)
    # The reference clears on any unusable feed or calendar (a gap, restart or
    # closed session starts the warm-up again); otherwise it follows the last
    # accepted quote, as the observer protocol describes.
    if feed_v["verdict"] == UNUSABLE or calendar["verdict"] == UNUSABLE:
        state.prior = None
    elif parsed is not None:
        state.prior = parsed
    return {"schema": SCHEMA, "config_digest": cfg.digest(), "now_utc": now.isoformat(),
            "verdicts": {v["input"]: v for v in (feed_v, quote_v, ref_v, calendar)}}


def gate(record: dict[str, Any], required: tuple[str, ...] = ("feed", "quote", "calendar"),
         purpose: str = "entry") -> dict[str, Any]:
    """Entry needs every required input usable. An exit never waits on the judge."""
    if purpose not in ("entry", "exit"):
        raise ValueError("purpose must be 'entry' or 'exit'")
    verdicts = record["verdicts"]
    unknown = [n for n in required if n not in verdicts]
    if unknown:
        raise ValueError(f"unknown required input: {unknown}")
    blocking = [{"input": n, "verdict": verdicts[n]["verdict"], "reasons": verdicts[n]["reasons"]}
                for n in required if verdicts[n]["verdict"] != USABLE]
    if purpose == "exit":
        return {"allow": True, "purpose": purpose, "abstain_reason": None, "advisory": blocking}
    reason = None
    if blocking:
        first = blocking[0]
        reason = f"{first['input']}:{first['reasons'][0]}" if first["reasons"] else first["input"]
    return {"allow": not blocking, "purpose": purpose, "abstain_reason": reason, "blocking": blocking}
