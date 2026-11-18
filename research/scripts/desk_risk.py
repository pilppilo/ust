"""Pre-submit risk gate for the paper desk.

Pure functions over an explicit state, so every charter row can be tested
without a broker. The runner calls this before creating an intent; the
supervisor uses the same state for stops and flattening. Exits are treated
differently from entries: a position must remain closable after a loss, a
halt or a wide spread.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
REGULAR_OPEN_MINUTE = 9 * 60 + 30
REGULAR_CLOSE_MINUTE = 16 * 60
MAX_QUOTE_FUTURE_SECONDS = 2.0
EXIT_SPREAD_MULTIPLE = 3.0


@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: float
    ask: float
    timestamp: datetime

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    @property
    def spread_bps(self) -> float:
        return (self.ask - self.bid) / self.mid * 10_000 if self.mid > 0 else math.inf


@dataclass(frozen=True)
class Position:
    symbol: str
    quantity: float
    average_entry: float
    sleeve: str
    market_price: float

    @property
    def notional(self) -> float:
        return abs(self.quantity) * self.market_price

    @property
    def side(self) -> str:
        return "long" if self.quantity > 0 else "short"


@dataclass(frozen=True)
class DeskState:
    positions: tuple[Position, ...]
    equity_usd: float
    day_start_equity_usd: float
    daily_pnl_usd: float
    traded_notional_today_usd: float
    open_orders_notional_usd: float
    open_orders: int
    unresolved_intents: int
    consecutive_losing_days: int

    @property
    def gross_notional(self) -> float:
        return sum(position.notional for position in self.positions)

    def sleeve_notional(self, sleeve: str) -> float:
        return sum(position.notional for position in self.positions if position.sleeve == sleeve)

    def position_for(self, symbol: str) -> Position | None:
        for position in self.positions:
            if position.symbol == symbol:
                return position
        return None


def _et(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError("decision clock must include a timezone")
    return now.astimezone(ET)


def _session_minute(now: datetime) -> int:
    local = _et(now)
    return local.hour * 60 + local.minute


def _clock_minute(value: str) -> int:
    hour, minute = map(int, value.split(":"))
    return hour * 60 + minute


def quote_age_seconds(quote: Quote, now: datetime) -> float:
    stamp = quote.timestamp
    if stamp.tzinfo is None:
        raise ValueError("quote timestamp must include a timezone")
    return (now.astimezone(timezone.utc) - stamp.astimezone(timezone.utc)).total_seconds()


def entry_window_reason(config: dict, now: datetime) -> str | None:
    local = _et(now)
    if local.weekday() >= 5:
        return "outside_regular_session"
    minute = _session_minute(now)
    if minute < REGULAR_OPEN_MINUTE or minute >= REGULAR_CLOSE_MINUTE:
        return "outside_regular_session"
    if minute < _clock_minute(config["no_new_entries_before_et"]):
        return "before_entry_start"
    if minute >= _clock_minute(config["no_new_entries_after_et"]):
        return "past_entry_cutoff"
    return None


def check_quote(config: dict, quote: Quote, now: datetime, *, for_entry: bool = True) -> str | None:
    if quote.bid <= 0 or quote.ask <= 0 or quote.ask < quote.bid:
        return "invalid_quote"
    age = quote_age_seconds(quote, now)
    if age < -MAX_QUOTE_FUTURE_SECONDS or age > float(config["max_quote_age_seconds"]):
        return "quote_stale_or_future"
    if for_entry and quote.spread_bps > float(config["max_spread_bps"]):
        return "spread_cap"
    if not for_entry and quote.spread_bps > float(config["max_spread_bps"]) * EXIT_SPREAD_MULTIPLE:
        return "exit_spread_emergency"
    return None


def check_exit(config: dict, state: DeskState, quote: Quote, position: Position,
               quantity: float, now: datetime) -> tuple[bool, str]:
    if position.symbol != quote.symbol:
        return False, "quote_symbol_mismatch"
    if position.quantity == 0:
        return False, "no_position"
    if not float(quantity).is_integer() or quantity <= 0:
        return False, "quantity_not_whole"
    if quantity > abs(position.quantity) + 1e-9:
        return False, "exceeds_position"
    reason = check_quote(config, quote, now, for_entry=False)
    if reason:
        return False, reason
    return True, "approved"


def size_entry(config: dict, state: DeskState, quote: Quote, sleeve: str, side: str = "buy") -> int:
    """Whole shares that fit every cap; zero when nothing fits."""
    price = quote.ask if side == "buy" else quote.bid
    if price <= 0:
        return 0
    remaining = min(
        float(config["max_position_notional_usd"]),
        float(config["max_gross_notional_usd"]) - state.gross_notional,
        float(config["sleeve_budgets"][sleeve]) - state.sleeve_notional(sleeve),
        float(config["max_daily_traded_notional_usd"])
        - state.traded_notional_today_usd - state.open_orders_notional_usd,
    )
    if remaining <= 0:
        return 0
    return int(math.floor(remaining / price))


def check_entry(config: dict, state: DeskState, quote: Quote, *, symbol: str, sleeve: str,
                quantity: float, now: datetime, side: str = "buy") -> tuple[bool, str]:
    if side not in {"buy", "sell"}:
        return False, "invalid_side"
    if not config.get("enabled") or not config.get("network_submission_enabled"):
        return False, "desk_disabled"
    if config.get("kill_switch"):
        return False, "kill_switch_on"
    if _et(now).date().isoformat() > str(config["observation_end_date"]):
        return False, "observation_window_expired"
    reason = entry_window_reason(config, now)
    if reason:
        return False, reason
    if symbol not in config["symbols"] or quote.symbol != symbol:
        return False, "symbol_not_allowed"
    if sleeve not in config["sleeve_budgets"]:
        return False, "sleeve_unknown"
    if side == "sell" and not config.get("allow_short"):
        return False, "short_not_allowed"
    reason = check_quote(config, quote, now, for_entry=True)
    if reason:
        return False, reason
    if state.unresolved_intents:
        return False, "unresolved_intent"
    if state.open_orders > int(config["max_open_orders"]):
        return False, "open_order_present"
    if state.daily_pnl_usd <= -float(config["max_daily_loss_usd"]):
        return False, "daily_loss_cap"
    if state.consecutive_losing_days >= int(config["max_consecutive_losing_days"]):
        return False, "consecutive_loss_halt"
    if not float(quantity).is_integer() or quantity <= 0:
        return False, "quantity_not_whole"

    existing = state.position_for(symbol)
    if existing is None:
        if len(state.positions) >= int(config["max_open_positions"]):
            return False, "position_count_cap"
        projected_quantity = quantity if side == "buy" else -quantity
    else:
        if existing.sleeve != sleeve:
            return False, "sleeve_mismatch"
        if (side == "buy" and existing.quantity < 0) or (side == "sell" and existing.quantity > 0):
            return False, "opposes_position"
        projected_quantity = existing.quantity + (quantity if side == "buy" else -quantity)

    price = quote.ask if side == "buy" else quote.bid
    order_notional = quantity * price
    if abs(projected_quantity) * price > float(config["max_position_notional_usd"]) + 1e-9:
        return False, "position_cap"
    if state.gross_notional + order_notional > float(config["max_gross_notional_usd"]) + 1e-9:
        return False, "gross_cap"
    if state.sleeve_notional(sleeve) + order_notional > float(config["sleeve_budgets"][sleeve]) + 1e-9:
        return False, "sleeve_cap"
    spent = state.traded_notional_today_usd + state.open_orders_notional_usd + order_notional
    if spent > float(config["max_daily_traded_notional_usd"]) + 1e-9:
        return False, "daily_traded_notional_cap"
    return True, "approved"


def position_stop_reason(config: dict, position: Position, quote: Quote) -> str | None:
    """True when the position is beyond its per-position adverse stop."""
    if position.quantity == 0 or position.average_entry <= 0:
        return None
    if position.quantity > 0:
        move_bps = (quote.bid / position.average_entry - 1) * 10_000
    else:
        move_bps = (position.average_entry / quote.ask - 1) * 10_000
    if move_bps <= -float(config["max_position_loss_bps"]):
        return "position_stop"
    return None


def flatten_order(position: Position) -> tuple[str, float]:
    return ("sell" if position.quantity > 0 else "buy"), abs(position.quantity)


def next_session_open(now: datetime) -> datetime:
    local = _et(now)
    probe = local.replace(hour=9, minute=30, second=0, microsecond=0)
    if local.weekday() < 5 and local < probe:
        return probe
    probe += timedelta(days=1)
    while probe.weekday() >= 5:
        probe += timedelta(days=1)
    return probe
