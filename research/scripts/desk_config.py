"""Schema v2/v3 validation for the bounded paper desk.

This is deliberately separate from paper_loop.validate_config. The probe's
schema v1 keeps its exact SPY-only meaning; the desk gets its own contract.

Schema v2 is the intraday nine-sector-ETF desk. Schema v3 adds named lanes;
only the `slow` lane is defined, with its own weekly book, percentage name
stop and sleeve-loss stop. The two schemas are validated separately so a v3
document cannot silently inherit an intraday control such as the 15:50
force-flat.

Hard bounds are the charter values. A configuration may lower them, never
raise them, and the loader rejects unknown fields so a typo cannot silently
disable a control.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

PAPER_HOST = "paper-api.alpaca.markets"
SYMBOLS = ("XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY")

# Charter maxima. Equality is allowed; anything above is a deployment error.
HARD_MAX = {
    "nav_usd": 25_000.0,
    "max_gross_notional_usd": 22_500.0,
    "max_position_notional_usd": 4_500.0,
    "max_open_positions": 5,
    "max_daily_traded_notional_usd": 50_000.0,
    "max_daily_loss_usd": 250.0,
    "max_position_loss_bps": 50.0,
    "max_consecutive_losing_days": 3,
    "max_spread_bps": 10.0,
    "max_quote_age_seconds": 5.0,
    "max_reference_age_seconds": 60.0,
}
HARD_MIN = {
    "nav_usd": 25_000.0,
    "max_gross_notional_usd": 1_000.0,
    "max_position_notional_usd": 100.0,
    "max_open_positions": 1,
    "max_daily_traded_notional_usd": 1_000.0,
    "max_daily_loss_usd": 10.0,
    "max_position_loss_bps": 5.0,
    "max_consecutive_losing_days": 1,
    "max_spread_bps": 1.0,
    "max_quote_age_seconds": 1.0,
    "max_reference_age_seconds": 5.0,
}
SLEEVE_MAX = {"E": 6_250.0, "S": 18_750.0}
SCHEMA_VERSION_V2 = 2
SCHEMA_VERSION_V3 = 3
SLOW_LANE = "slow"
SLOW_TARGET_NOTIONAL_USD = 937.50
SLOW_MAX_POSITION_MULTIPLE = 2.0
SLOW_MAX_PRICE_USD = SLOW_TARGET_NOTIONAL_USD * SLOW_MAX_POSITION_MULTIPLE
SLOW_MAX_OPEN_POSITIONS = 20
SLOW_MAX_NAME_STOP_PCT = 15.0
SLOW_MAX_WEEKLY_LOSS_FRACTION = 0.04
SLOW_ENTRY_WINDOW = ("15:58", "15:59")
SLOW_LANE_FIELDS = {
    "sleeve", "symbols", "budget_usd", "max_open_positions",
    "target_position_notional_usd", "max_position_notional_usd",
    "max_position_loss_pct", "weekly_loss_usd", "max_consecutive_losing_weeks",
    "max_spread_bps", "entry_window_et", "exit_window_et", "rebalance",
    "hold", "force_flat", "daily_loss_applies", "sizing",
}
SLOW_SIZING_FIELDS = {"mode", "min_shares", "max_price_usd"}
V3_REQUIRED = {
    "schema_version", "kind", "config_revision", "enabled",
    "network_submission_enabled", "kill_switch", "observation_start_date",
    "observation_end_date", "adapter", "endpoint", "nav_usd",
    "max_gross_notional_usd", "max_daily_traded_notional_usd", "max_open_orders",
    "order_type", "time_in_force", "allow_short", "max_quote_age_seconds",
    "max_reference_age_seconds", "lanes", "candidates",
}
V3_CANDIDATE_FIELDS = {
    "id", "lane", "symbols", "direction", "parameters",
    "predeclaration_sha256", "screen_evidence",
}
REQUIRED = {
    "schema_version", "kind", "config_revision", "enabled",
    "network_submission_enabled", "kill_switch", "observation_end_date",
    "adapter", "endpoint", "symbols", "nav_usd", "max_gross_notional_usd",
    "max_position_notional_usd", "max_open_positions",
    "max_daily_traded_notional_usd", "max_daily_loss_usd",
    "max_position_loss_bps", "max_consecutive_losing_days",
    "no_new_entries_before_et", "no_new_entries_after_et", "force_flat_after_et",
    "max_spread_bps",
    "max_quote_age_seconds", "max_reference_age_seconds", "max_open_orders",
    "order_type", "time_in_force", "allow_short", "sleeve_budgets",
    "candidates",
}


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def desk_config_digest(config: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(config)).hexdigest()


def _finite(value: object, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"boolean is not numeric: {name}")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"not numeric: {name}") from error
    if not math.isfinite(number):
        raise ValueError(f"non-finite: {name}")
    return number


def _integer(value: object, name: str) -> int:
    number = _finite(value, name)
    if number != int(number):
        raise ValueError(f"must be an integer: {name}")
    return int(number)


def _clock_minute(value: object) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"\d{2}:\d{2}", value):
        raise ValueError("session time must be HH:MM")
    hour, minute = map(int, value.split(":"))
    if not (0 <= hour < 24 and 0 <= minute < 60):
        raise ValueError("session time out of range")
    return hour * 60 + minute


def _validate_v2(config: dict[str, Any]) -> None:
    if not isinstance(config, dict):
        raise ValueError("desk config must be an object")
    if set(config) != REQUIRED:
        missing = sorted(REQUIRED - set(config))
        extra = sorted(set(config) - REQUIRED)
        raise ValueError(f"desk config fields mismatch; missing={missing} extra={extra}")
    if config["schema_version"] != 2 or config["kind"] != "paper_desk":
        raise ValueError("unsupported desk config schema")
    if config["adapter"] != "alpaca_paper":
        raise ValueError("unsupported adapter")
    endpoint = urlparse(str(config["endpoint"]))
    if (endpoint.scheme != "https" or endpoint.hostname != PAPER_HOST
            or endpoint.username is not None or endpoint.password is not None
            or endpoint.port not in (None, 443)
            or endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment):
        raise ValueError("endpoint must be the exact Alpaca paper host")
    if tuple(config["symbols"]) != SYMBOLS:
        raise ValueError("desk scope is the fixed nine-sector-ETF list")
    for name in ("enabled", "network_submission_enabled", "kill_switch", "allow_short"):
        if not isinstance(config[name], bool):
            raise ValueError(f"must be boolean: {name}")
    revision = _integer(config["config_revision"], "config_revision")
    if revision < 1:
        raise ValueError("config_revision must be positive")
    try:
        datetime.fromisoformat(str(config["observation_end_date"]))
    except ValueError as error:
        raise ValueError("observation_end_date must be ISO date") from error

    for name, ceiling in HARD_MAX.items():
        value = _finite(config[name], name)
        if value < HARD_MIN[name]:
            raise ValueError(f"below reviewed minimum: {name}")
        if value > ceiling:
            raise ValueError(f"above charter maximum: {name}")
        if name in {"max_open_positions", "max_consecutive_losing_days"}:
            _integer(value, name)

    nav = _finite(config["nav_usd"], "nav_usd")
    gross = _finite(config["max_gross_notional_usd"], "max_gross_notional_usd")
    position = _finite(config["max_position_notional_usd"], "max_position_notional_usd")
    if gross > nav * 0.9 + 1e-9:
        raise ValueError("gross cap exceeds 0.9x NAV")
    if position > nav * 0.18 + 1e-9:
        raise ValueError("position cap exceeds 18% NAV")
    budget = gross
    if position * _integer(config["max_open_positions"], "max_open_positions") < budget - 1e-9:
        raise ValueError("position cap times position count cannot reach the gross cap")

    no_entry = _clock_minute(config["no_new_entries_after_et"])
    entry_start = _clock_minute(config["no_new_entries_before_et"])
    flat = _clock_minute(config["force_flat_after_et"])
    if not (9 * 60 + 30 <= entry_start <= 9 * 60 + 35):
        raise ValueError("entry start must be between 09:30 and 09:35 ET")
    if not (9 * 60 + 30 <= no_entry <= 15 * 60 + 30 and entry_start < no_entry < flat <= 15 * 60 + 50):
        raise ValueError("session controls outside the charter window")
    if _finite(config["max_spread_bps"], "max_spread_bps") > 10:
        raise ValueError("spread gate above charter")
    if _finite(config["max_quote_age_seconds"], "max_quote_age_seconds") > 5:
        raise ValueError("quote age above charter")
    if _finite(config["max_reference_age_seconds"], "max_reference_age_seconds") > 60:
        raise ValueError("reference age above charter")

    if _integer(config["max_open_orders"], "max_open_orders") != 0:
        raise ValueError("desk is market-order only; resting orders are not permitted")
    if config["order_type"] != "market" or config["time_in_force"] != "day":
        raise ValueError("desk orders are day market orders")

    sleeves = config["sleeve_budgets"]
    if not isinstance(sleeves, dict) or set(sleeves) != set(SLEEVE_MAX):
        raise ValueError("sleeve budgets must define exactly E and S")
    total = 0.0
    for name, ceiling in SLEEVE_MAX.items():
        value = _finite(sleeves[name], f"sleeve {name}")
        if value <= 0 or value > ceiling:
            raise ValueError(f"sleeve budget outside charter: {name}")
        total += value
    # Budgets are funding allocations, not simultaneous entitlements. The
    # gross cap above is what binds at any instant, so the budgets may sum to
    # NAV while gross stays at 0.9x.
    if total > nav + 1e-9:
        raise ValueError("sleeve budgets exceed the funding base")

    candidates = config["candidates"]
    if not isinstance(candidates, list):
        raise ValueError("candidates must be a list")
    seen: set[str] = set()
    experimental = 0
    for candidate in candidates:
        if not isinstance(candidate, dict) or set(candidate) - {
                "id", "sleeve", "symbols", "direction", "parameters",
                "predeclaration_sha256", "screen_evidence"}:
            raise ValueError("candidate has unsupported fields")
        required = {"id", "sleeve", "symbols", "direction", "parameters", "predeclaration_sha256"}
        if not required.issubset(candidate):
            raise ValueError("candidate is missing required fields")
        identifier = candidate["id"]
        if not isinstance(identifier, str) or not re.fullmatch(r"[a-z0-9][a-z0-9._-]{2,40}", identifier):
            raise ValueError("candidate id is not canonical")
        if identifier in seen:
            raise ValueError("duplicate candidate id")
        seen.add(identifier)
        sleeve = candidate["sleeve"]
        if sleeve not in SLEEVE_MAX:
            raise ValueError("candidate sleeve must be E or S")
        if sleeve == "E":
            experimental += 1
        symbols = candidate["symbols"]
        if (not isinstance(symbols, list) or not symbols
                or any(symbol not in SYMBOLS for symbol in symbols)):
            raise ValueError("candidate symbols must be a non-empty subset of the desk scope")
        direction = candidate["direction"]
        if direction not in {"long", "short"}:
            raise ValueError("candidate direction must be long or short")
        if direction == "short" and not config["allow_short"]:
            raise ValueError("short candidate while allow_short is false")
        if not isinstance(candidate["parameters"], dict):
            raise ValueError("candidate parameters must be an object")
        for key, value in candidate["parameters"].items():
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                _finite(value, f"parameter {key}")
        digest = candidate["predeclaration_sha256"]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("candidate needs a predeclaration SHA-256")
        if sleeve == "S":
            evidence = candidate.get("screen_evidence")
            if (not isinstance(evidence, dict)
                    or not isinstance(evidence.get("decision_record"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", str(evidence.get("sha256", "")))):
                raise ValueError("selected-sleeve candidate needs screen evidence")
    if experimental > 1:
        raise ValueError("experimental sleeve runs one predeclared rule at a time")


def _validate_v3(config: dict[str, Any]) -> None:
    if not isinstance(config, dict):
        raise ValueError("desk config must be an object")
    if set(config) != V3_REQUIRED:
        missing = sorted(V3_REQUIRED - set(config))
        extra = sorted(set(config) - V3_REQUIRED)
        raise ValueError(f"desk config fields mismatch; missing={missing} extra={extra}")
    if config["schema_version"] != SCHEMA_VERSION_V3 or config["kind"] != "paper_desk":
        raise ValueError("unsupported desk config schema")
    if config["adapter"] != "alpaca_paper":
        raise ValueError("unsupported adapter")
    endpoint = urlparse(str(config["endpoint"]))
    if (endpoint.scheme != "https" or endpoint.hostname != PAPER_HOST
            or endpoint.username is not None or endpoint.password is not None
            or endpoint.port not in (None, 443)
            or endpoint.path not in ("", "/") or endpoint.query or endpoint.fragment):
        raise ValueError("endpoint must be the exact Alpaca paper host")
    for name in ("enabled", "network_submission_enabled", "kill_switch", "allow_short"):
        if not isinstance(config[name], bool):
            raise ValueError(f"must be boolean: {name}")
    revision = _integer(config["config_revision"], "config_revision")
    if revision < 1:
        raise ValueError("config_revision must be positive")
    try:
        start = date.fromisoformat(str(config["observation_start_date"]))
        end = date.fromisoformat(str(config["observation_end_date"]))
    except ValueError as error:
        raise ValueError("observation dates must be ISO dates") from error
    if start > end:
        raise ValueError("observation window starts after it ends")

    nav = _finite(config["nav_usd"], "nav_usd")
    if nav != HARD_MAX["nav_usd"]:
        raise ValueError("schema v3 is fixed to the reviewed $25,000 paper NAV")
    gross = _finite(config["max_gross_notional_usd"], "max_gross_notional_usd")
    if not (HARD_MIN["max_gross_notional_usd"] <= gross <= HARD_MAX["max_gross_notional_usd"]):
        raise ValueError("gross cap outside charter")
    if gross > nav * 0.9 + 1e-9:
        raise ValueError("gross cap exceeds 0.9x NAV")
    traded = _finite(config["max_daily_traded_notional_usd"], "max_daily_traded_notional_usd")
    if not (HARD_MIN["max_daily_traded_notional_usd"] <= traded
            <= HARD_MAX["max_daily_traded_notional_usd"]):
        raise ValueError("daily traded cap outside charter")
    quote_age = _finite(config["max_quote_age_seconds"], "max_quote_age_seconds")
    if not (HARD_MIN["max_quote_age_seconds"] <= quote_age <= 5.0):
        raise ValueError("quote age outside charter")
    reference_age = _finite(config["max_reference_age_seconds"], "max_reference_age_seconds")
    if not (HARD_MIN["max_reference_age_seconds"] <= reference_age <= 60.0):
        raise ValueError("reference age outside charter")
    if _integer(config["max_open_orders"], "max_open_orders") != 0:
        raise ValueError("desk is market-order only; resting orders are not permitted")
    if config["order_type"] != "market" or config["time_in_force"] != "day":
        raise ValueError("desk orders are day market orders")

    lanes = config["lanes"]
    if not isinstance(lanes, dict) or set(lanes) != {SLOW_LANE}:
        raise ValueError("schema v3 supports exactly the slow lane")
    slow = lanes[SLOW_LANE]
    if not isinstance(slow, dict) or set(slow) != SLOW_LANE_FIELDS:
        raise ValueError("slow lane fields mismatch")

    if slow["sleeve"] != "S":
        raise ValueError("slow lane must use the selected sleeve")
    symbols = slow["symbols"]
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("slow lane symbols must be a non-empty list")
    if len(set(symbols)) != len(symbols):
        raise ValueError("slow lane symbols must be unique")
    for symbol in symbols:
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z][A-Z0-9]{0,6}(?:\.[A-Z])?", symbol):
            raise ValueError(f"slow lane symbol is not canonical: {symbol!r}")

    count = _integer(slow["max_open_positions"], "max_open_positions")
    if count < 2 or count > SLOW_MAX_OPEN_POSITIONS:
        raise ValueError("slow lane position count outside the annex")
    if count % 2:
        raise ValueError("slow lane book must pair long and short positions")
    if count > len(symbols):
        raise ValueError("slow lane has fewer symbols than positions")
    target = _finite(slow["target_position_notional_usd"], "target_position_notional_usd")
    if not (0 < target <= SLOW_TARGET_NOTIONAL_USD + 1e-9):
        raise ValueError("slow lane target notional outside the annex")
    budget = _finite(slow["budget_usd"], "budget_usd")
    if not (0 < budget <= SLEEVE_MAX["S"] + 1e-9):
        raise ValueError("slow lane budget outside the annex")
    if abs(budget - target * count) > 0.01:
        raise ValueError("slow lane budget must equal target times position count")
    if gross < budget - 1e-9:
        raise ValueError("gross cap cannot fund the slow lane book")
    if traded < 2 * budget - 1e-9:
        raise ValueError("daily traded cap cannot turn the slow lane book over")
    max_position = _finite(slow["max_position_notional_usd"], "max_position_notional_usd")
    if abs(max_position - target * SLOW_MAX_POSITION_MULTIPLE) > 1e-6:
        raise ValueError("slow lane position cap must be exactly 2x the target")
    if max_position > SLOW_MAX_PRICE_USD + 1e-9:
        raise ValueError("slow lane position cap above the annex")

    stop_pct = _finite(slow["max_position_loss_pct"], "max_position_loss_pct")
    if not (0 < stop_pct <= SLOW_MAX_NAME_STOP_PCT):
        raise ValueError("slow lane name stop above the annex")
    weekly_loss = _finite(slow["weekly_loss_usd"], "weekly_loss_usd")
    if weekly_loss <= 0 or weekly_loss > min(
            budget * SLOW_MAX_WEEKLY_LOSS_FRACTION,
            SLEEVE_MAX["S"] * SLOW_MAX_WEEKLY_LOSS_FRACTION) + 1e-6:
        raise ValueError("slow lane weekly loss stop above 4% of the sleeve")
    losing_weeks = _integer(slow["max_consecutive_losing_weeks"],
                            "max_consecutive_losing_weeks")
    if not (1 <= losing_weeks <= 3):
        raise ValueError("slow lane losing-week halt outside the annex")
    spread = _finite(slow["max_spread_bps"], "max_spread_bps")
    if not (HARD_MIN["max_spread_bps"] <= spread <= HARD_MAX["max_spread_bps"]):
        raise ValueError("slow lane spread gate outside charter")

    for name in ("entry_window_et", "exit_window_et"):
        window = slow[name]
        if not isinstance(window, (list, tuple)) or len(window) != 2:
            raise ValueError(f"{name} must be a two-element window")
        for value in window:
            _clock_minute(value)
        if tuple(window) != SLOW_ENTRY_WINDOW:
            raise ValueError("slow lane rebalance window must be 15:58-15:59 ET")
    if slow["rebalance"] != "first_trading_day":
        raise ValueError("slow lane must rebalance on the first trading day")
    if slow["hold"] != "weekly":
        raise ValueError("slow lane must hold for one week")
    for name in ("force_flat", "daily_loss_applies"):
        if not isinstance(slow[name], bool):
            raise ValueError(f"must be boolean: slow {name}")
        if slow[name]:
            raise ValueError(f"slow lane must not inherit intraday {name}")

    sizing = slow["sizing"]
    if not isinstance(sizing, dict) or set(sizing) != SLOW_SIZING_FIELDS:
        raise ValueError("slow lane sizing fields mismatch")
    if sizing["mode"] != "whole_share_target":
        raise ValueError("slow lane sizing must be whole_share_target")
    if _integer(sizing["min_shares"], "sizing min_shares") != 1:
        raise ValueError("slow lane minimum is one whole share")
    max_price = _finite(sizing["max_price_usd"], "sizing max_price_usd")
    if abs(max_price - max_position) > 1e-6 or max_price > SLOW_MAX_PRICE_USD + 1e-9:
        raise ValueError("slow lane sizing price cap must be 2x the target")

    candidates = config["candidates"]
    if not isinstance(candidates, list):
        raise ValueError("candidates must be a list")
    seen_ids: set[str] = set()
    seen_lanes: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, dict) or set(candidate) - V3_CANDIDATE_FIELDS:
            raise ValueError("candidate has unsupported fields")
        required = {"id", "lane", "symbols", "direction", "parameters",
                    "predeclaration_sha256"}
        if not required.issubset(candidate):
            raise ValueError("candidate is missing required fields")
        identifier = candidate["id"]
        if not isinstance(identifier, str) or not re.fullmatch(
                r"[a-z0-9][a-z0-9._-]{2,40}", identifier):
            raise ValueError("candidate id is not canonical")
        if identifier in seen_ids:
            raise ValueError("duplicate candidate id")
        seen_ids.add(identifier)
        lane = candidate["lane"]
        if lane not in lanes:
            raise ValueError("candidate lane is not declared")
        if lane in seen_lanes:
            raise ValueError("one candidate per lane")
        seen_lanes.add(lane)
        chosen = candidate["symbols"]
        if (not isinstance(chosen, list) or len(chosen) < count
                or any(symbol not in symbols for symbol in chosen)):
            raise ValueError("candidate symbols must cover the lane scope")
        direction = candidate["direction"]
        if direction != "long_short":
            raise ValueError("slow lane candidate must be long_short")
        if direction == "long_short" and not config["allow_short"]:
            raise ValueError("long_short candidate while allow_short is false")
        if not isinstance(candidate["parameters"], dict):
            raise ValueError("candidate parameters must be an object")
        for key, value in candidate["parameters"].items():
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                _finite(value, f"parameter {key}")
        digest = candidate["predeclaration_sha256"]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("candidate needs a predeclaration SHA-256")
        evidence = candidate.get("screen_evidence")
        if (not isinstance(evidence, dict)
                or not isinstance(evidence.get("decision_record"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", str(evidence.get("sha256", "")))):
            raise ValueError("selected-sleeve candidate needs screen evidence")


def validate_desk_config(config: dict[str, Any]) -> None:
    if isinstance(config, dict) and config.get("schema_version") == SCHEMA_VERSION_V3:
        _validate_v3(config)
        return
    _validate_v2(config)


def require_order_path_schema(config: dict[str, Any],
                              version: int = SCHEMA_VERSION_V2) -> None:
    """Fail closed until an order path for the document's schema exists.

    Schema v3 is validated for inspection and shadow accounting only. No
    supervisor, runner or close job may act on it until the lane rules are
    wired and tested.
    """
    validate_desk_config(config)
    actual = config["schema_version"]
    if actual != version:
        raise ValueError(
            f"order path requires schema v{version}; schema v{actual} has no wired order path")


def slow_lane(config: dict[str, Any]) -> dict[str, Any]:
    validate_desk_config(config)
    if config["schema_version"] != SCHEMA_VERSION_V3:
        raise ValueError("no slow lane in schema v2")
    return config["lanes"][SLOW_LANE]


def sleeve_budget(config: dict[str, Any], sleeve: str) -> float:
    validate_desk_config(config)
    if config["schema_version"] == SCHEMA_VERSION_V3:
        for lane in config["lanes"].values():
            if lane["sleeve"] == sleeve:
                return float(lane["budget_usd"])
        raise ValueError(f"sleeve not declared: {sleeve}")
    return float(config["sleeve_budgets"][sleeve])


def known_sleeves(config: dict[str, Any]) -> frozenset[str]:
    validate_desk_config(config)
    if config["schema_version"] == SCHEMA_VERSION_V3:
        return frozenset(str(lane["sleeve"]) for lane in config["lanes"].values())
    return frozenset(str(sleeve) for sleeve in config["sleeve_budgets"])


def desk_symbols(config: dict[str, Any]) -> tuple[str, ...]:
    """The order scope the adapter must accept for this document."""
    validate_desk_config(config)
    if config["schema_version"] == SCHEMA_VERSION_V3:
        return tuple(str(symbol) for symbol in config["lanes"][SLOW_LANE]["symbols"])
    return tuple(SYMBOLS)


def load_desk_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    validate_desk_config(config)
    return config


def today_utc() -> str:
    return date.today().isoformat()
