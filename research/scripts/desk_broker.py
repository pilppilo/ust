"""Nine-symbol paper broker adapter for the desk.

Deliberately separate from paper_loop.AlpacaPaperAdapter: that adapter is
SPY-hardcoded and its verified_state contract assumes one position. This one
reads the fixed desk scope, returns typed errors so the supervisor can tell a
stale feed from a dead broker, and never retries an ambiguous submit.

Reads are split on purpose. Positions and open orders are critical for exits;
daily P&L is not. A failed P&L read must halt new entries, not block a stop.
"""
from __future__ import annotations

import json
import math
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse

from desk_config import PAPER_HOST, SYMBOLS
from desk_risk import Quote

DATA_HOST = "data.alpaca.markets"
MAX_RESPONSE_BYTES = 1_048_576
REQUEST_TIMEOUT_SECONDS = 5.0
MAX_POSITIONS = 100
MAX_OPEN_ORDERS = 500
MAX_BARS = 20_000
BARS_PAGE_LIMIT = 5_000
MAX_BAR_PAGES = 6
TERMINAL_ORDER_STATUSES = frozenset({"filled", "canceled", "rejected", "expired"})


class BrokerError(RuntimeError):
    """Base class for broker failures the supervisor must classify."""


class BrokerUnavailable(BrokerError):
    """Transport, timeout, 5xx, 429 or an unparseable response. Retry reads."""


class BrokerRejected(BrokerError):
    """The broker refused the request; inspect before doing anything else."""


class BrokerStateError(BrokerError):
    """Broker truth is outside the desk contract. Escalate, do not trade."""


class OrderNotFound(BrokerError):
    """Lookup by client order ID returned 404."""


@dataclass(frozen=True)
class BrokerPosition:
    symbol: str
    quantity: float
    average_entry: float
    market_price: float


@dataclass(frozen=True)
class OpenOrder:
    client_order_id: str
    broker_order_id: str
    symbol: str
    side: str
    quantity: float
    filled_quantity: float
    status: str

    @property
    def remaining(self) -> float:
        return max(0.0, self.quantity - self.filled_quantity)


@dataclass(frozen=True)
class MinuteBar:
    symbol: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class BrokerState:
    equity_usd: float
    positions: tuple[BrokerPosition, ...]
    open_orders: tuple[OpenOrder, ...]

    @property
    def open_order_symbols(self) -> frozenset[str]:
        return frozenset(order.symbol for order in self.open_orders if order.remaining > 0)


def _finite(value: object, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise BrokerStateError(f"not numeric: {name}") from error
    if not math.isfinite(number):
        raise BrokerStateError(f"non-finite: {name}")
    return number


def _aware(value: object, name: str) -> datetime:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as error:
        raise BrokerStateError(f"invalid timestamp: {name}") from error
    if stamp.tzinfo is None:
        raise BrokerStateError(f"timestamp missing timezone: {name}")
    return stamp


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


class AlpacaDeskAdapter:
    """Paper-only REST adapter for the fixed nine-ETF desk scope."""

    def __init__(self, endpoint: str, network_submission_enabled: bool = False,
                 allowed_symbols: Iterable[str] | None = None):
        parsed = urlparse(endpoint)
        if (parsed.scheme != "https" or parsed.hostname != PAPER_HOST
                or parsed.username is not None or parsed.password is not None
                or parsed.port not in (None, 443) or parsed.path not in ("", "/")
                or parsed.query or parsed.fragment):
            raise ValueError("adapter endpoint must be the exact Alpaca paper host")
        scope = tuple(SYMBOLS if allowed_symbols is None else allowed_symbols)
        if not scope or len(set(scope)) != len(scope):
            raise ValueError("adapter symbol scope must be non-empty and unique")
        for symbol in scope:
            if not isinstance(symbol, str) or not re.fullmatch(
                    r"[A-Z][A-Z0-9]{0,6}(?:\.[A-Z])?", symbol):
                raise ValueError(f"adapter symbol is not canonical: {symbol!r}")
        self.allowed_symbols = frozenset(scope)
        self.endpoint = "https://" + PAPER_HOST
        self.network_submission_enabled = network_submission_enabled
        self.last_calls: list[dict[str, Any]] = []
        self._key = ""
        self._secret = ""

    # -- transport ---------------------------------------------------------
    def _credentials(self) -> tuple[str, str]:
        if not self._key or not self._secret:
            # Imported lazily so offline replay stays dependency-free.
            from alpaca_live_iex_news_collector import read_credentials
            self._key, self._secret = read_credentials()
        if not self._key or not self._secret:
            raise BrokerUnavailable("paper credentials unavailable")
        return self._key, self._secret

    def _record(self, path: str, method: str, seconds: float,
                error_type: str | None = None) -> None:
        if len(self.last_calls) < 64:
            self.last_calls.append({"path": path.split("?", 1)[0], "method": method,
                                    "seconds": round(max(0.0, seconds), 3),
                                    "error_type": error_type})

    def _request(self, path: str, *, host: str = PAPER_HOST, method: str = "GET",
                 payload: dict[str, Any] | None = None) -> Any:
        if host not in {PAPER_HOST, DATA_HOST} or not path.startswith("/") or ".." in path:
            raise ValueError("request target is outside approved Alpaca paper/data hosts")
        key, secret = self._credentials()
        body = None if payload is None else _canonical(payload)
        request = urllib.request.Request(f"https://{host}{path}", data=body, method=method, headers={
            "APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret, "Accept": "application/json",
            **({"Content-Type": "application/json"} if body is not None else {}),
        })
        opener = urllib.request.build_opener(_NoRedirect())
        started = time.perf_counter()
        try:
            with opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise BrokerUnavailable("Alpaca response exceeds 1 MiB cap")
                result = json.loads(raw) if raw else {}
            self._record(path, method, time.perf_counter() - started)
            return result
        except HTTPError as error:
            self._record(path, method, time.perf_counter() - started,
                         error_type=type(error).__name__)
            if error.code == 404:
                raise OrderNotFound("broker object not found") from None
            if error.code == 429 or error.code >= 500:
                raise BrokerUnavailable(f"paper API unavailable ({error.code})") from None
            raise BrokerRejected(f"paper API rejected request ({error.code})") from None
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as error:
            self._record(path, method, time.perf_counter() - started,
                         error_type=type(error).__name__)
            # POST errors are ambiguous. The caller keeps the intent and
            # reconciles by client_order_id; it never resubmits blindly.
            raise BrokerUnavailable(
                f"paper API request failed ({type(error).__name__}); inspect by client_order_id"
            ) from None

    # -- reads -------------------------------------------------------------
    def market_clock(self) -> dict[str, Any]:
        clock = self._request("/v2/clock")
        if not isinstance(clock, dict) or not isinstance(clock.get("is_open"), bool):
            raise BrokerStateError("market clock is malformed")
        _aware(clock.get("timestamp"), "clock.timestamp")
        return clock

    def state(self) -> BrokerState:
        account = self._request("/v2/account")
        positions = self._request("/v2/positions")
        orders = self._request("/v2/orders?status=open&limit=500")
        if not isinstance(account, dict) or not isinstance(positions, list) or not isinstance(orders, list):
            raise BrokerStateError("account payload is malformed")
        if len(positions) > MAX_POSITIONS or len(orders) > MAX_OPEN_ORDERS:
            raise BrokerStateError("broker state exceeds configured response bounds")
        if account.get("status") != "ACTIVE" or any(
                account.get(key) for key in ("trading_blocked", "account_blocked", "trade_suspended_by_user")):
            raise BrokerStateError("paper account is not in an active unblocked state")
        parsed_positions = tuple(self._position(item) for item in positions)
        parsed_orders = tuple(self._order(item) for item in orders)
        return BrokerState(_finite(account.get("equity"), "account.equity"),
                           parsed_positions, parsed_orders)

    def _position(self, item: object) -> BrokerPosition:
        if not isinstance(item, dict):
            raise BrokerStateError("position payload is malformed")
        symbol = item.get("symbol")
        if symbol not in self.allowed_symbols:
            raise BrokerStateError(f"broker holds a symbol outside the desk scope: {symbol}")
        quantity = _finite(item.get("qty"), "position.qty")
        if quantity == 0:
            raise BrokerStateError("broker returned a zero position")
        return BrokerPosition(symbol, quantity,
                              _finite(item.get("avg_entry_price"), "position.avg_entry_price"),
                              _finite(item.get("current_price"), "position.current_price"))

    def _order(self, item: object) -> OpenOrder:
        if not isinstance(item, dict):
            raise BrokerStateError("order payload is malformed")
        symbol = item.get("symbol")
        if symbol not in self.allowed_symbols:
            raise BrokerStateError(f"broker order outside the desk scope: {symbol}")
        side = str(item.get("side", ""))
        if side not in {"buy", "sell"}:
            raise BrokerStateError("order side is not buy or sell")
        client_id = str(item.get("client_order_id", ""))
        if not client_id:
            raise BrokerStateError("open order has no client_order_id")
        quantity = _finite(item.get("qty"), "order.qty")
        filled = _finite(item.get("filled_qty") or 0, "order.filled_qty")
        if quantity <= 0 or filled < 0 or filled > quantity:
            raise BrokerStateError("open order quantity is outside bounds")
        return OpenOrder(client_id, str(item.get("id", "")), symbol, side,
                         quantity, filled, str(item.get("status", "")))

    def daily_pnl(self) -> float:
        history = self._request(
            "/v2/account/portfolio/history?period=1D&timeframe=1Min&extended_hours=false")
        if not isinstance(history, dict):
            raise BrokerStateError("portfolio history is malformed")
        pnl = history.get("profit_loss")
        if not isinstance(pnl, list) or not pnl:
            raise BrokerUnavailable("paper daily P&L unavailable; refusing new entries")
        return _finite(pnl[-1], "portfolio_history.profit_loss")

    def latest_quotes(self, symbols: Iterable[str]) -> dict[str, Quote]:
        wanted = tuple(symbols)
        if not wanted or any(symbol not in self.allowed_symbols for symbol in wanted):
            raise ValueError("quote scope must be a non-empty desk subset")
        path = "/v2/stocks/quotes/latest?symbols=" + ",".join(wanted) + "&feed=iex"
        result = self._request(path, host=DATA_HOST)
        if not isinstance(result, dict):
            raise BrokerStateError("quote payload is malformed")
        payload = result.get("quotes")
        if payload is None and isinstance(result.get("quote"), dict) and len(wanted) == 1:
            payload = {wanted[0]: result["quote"]}
        if not isinstance(payload, dict):
            raise BrokerStateError("quote payload has no quotes object")
        quotes: dict[str, Quote] = {}
        for symbol, raw in payload.items():
            if symbol not in wanted or not isinstance(raw, dict):
                continue
            try:
                quotes[symbol] = Quote(symbol, _finite(raw.get("bp"), "quote.bp"),
                                       _finite(raw.get("ap"), "quote.ap"),
                                       _aware(raw.get("t"), "quote.t"))
            except BrokerStateError:
                # A malformed symbol is omitted, not silently repaired. The
                # supervisor treats a missing quote as "do not enter".
                continue
        return quotes

    def minute_bars(self, symbols: Iterable[str], start: datetime,
                    end: datetime) -> dict[str, list[MinuteBar]]:
        """Bounded IEX 1-minute bars for the desk scope.

        Bars are time-stamped at their start. Callers must treat the bar that
        contains `now` as not final. Pagination is followed only to the page
        and total-bar caps; anything beyond fails closed.
        """
        wanted = tuple(symbols)
        if not wanted or any(symbol not in self.allowed_symbols for symbol in wanted):
            raise ValueError("bar scope must be a non-empty desk subset")
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("bar window must include a timezone")
        if end <= start:
            raise ValueError("bar window is empty")
        base = ("/v2/stocks/bars?symbols=" + ",".join(wanted)
                + "&timeframe=1Min"
                + "&start=" + urllib.parse.quote(start.astimezone(timezone.utc).isoformat(), safe="")
                + "&end=" + urllib.parse.quote(end.astimezone(timezone.utc).isoformat(), safe="")
                + f"&limit={BARS_PAGE_LIMIT}&adjustment=raw&feed=iex")
        collected: dict[str, list[MinuteBar]] = {symbol: [] for symbol in wanted}
        token: str | None = None
        pages = 0
        while True:
            path = base if token is None else base + "&page_token=" + urllib.parse.quote(token, safe="")
            payload = self._request(path, host=DATA_HOST)
            if not isinstance(payload, dict):
                raise BrokerStateError("bar payload is malformed")
            rows = payload.get("bars")
            if rows is None:
                rows = {}
            if not isinstance(rows, dict):
                raise BrokerStateError("bar payload has no bars object")
            for symbol, items in rows.items():
                if symbol not in wanted or not isinstance(items, list):
                    continue
                for item in items:
                    bar = self._bar(symbol, item)
                    if start <= bar.start <= end:
                        collected[symbol].append(bar)
            if sum(len(items) for items in collected.values()) > MAX_BARS:
                raise BrokerStateError("bar history exceeds the configured cap")
            pages += 1
            token = payload.get("next_page_token")
            if not token:
                break
            if pages >= MAX_BAR_PAGES:
                raise BrokerStateError("bar history exceeded the page cap")
        for items in collected.values():
            items.sort(key=lambda bar: bar.start)
        return collected

    @staticmethod
    def _bar(symbol: str, item: object) -> MinuteBar:
        if not isinstance(item, dict):
            raise BrokerStateError("bar is malformed")
        bar = MinuteBar(symbol, _aware(item.get("t"), "bar.t"),
                        _finite(item.get("o"), "bar.o"), _finite(item.get("h"), "bar.h"),
                        _finite(item.get("l"), "bar.l"), _finite(item.get("c"), "bar.c"),
                        _finite(item.get("v") or 0, "bar.v"))
        if bar.high < bar.low or bar.high < max(bar.open, bar.close) or bar.low > min(bar.open, bar.close):
            raise BrokerStateError("bar OHLC is inconsistent")
        if bar.volume < 0:
            raise BrokerStateError("bar volume is negative")
        return bar

    # -- writes ------------------------------------------------------------
    def prepare(self, intent: dict[str, Any]) -> dict[str, Any]:
        quantity = float(intent["quantity"])
        return {"method": "POST", "url": self.endpoint + "/v2/orders", "payload": {
            "symbol": intent["symbol"],
            "qty": format(quantity, ".8f").rstrip("0").rstrip("."),
            "side": intent["side"], "type": "market", "time_in_force": "day",
            "client_order_id": intent["intent_id"],
        }}

    def submit(self, request: dict[str, Any]) -> dict[str, Any]:
        if not self.network_submission_enabled:
            raise BrokerUnavailable("paper network submission is disabled")
        if request.get("method") != "POST" or request.get("url") != self.endpoint + "/v2/orders":
            raise ValueError("submit accepts only the exact paper orders endpoint")
        payload = request.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("order payload missing")
        if (payload.get("symbol") not in self.allowed_symbols
                or payload.get("side") not in {"buy", "sell"}
                or payload.get("type") != "market" or payload.get("time_in_force") != "day"
                or not str(payload.get("client_order_id", ""))
                or not float(payload.get("qty", 0)) > 0):
            raise ValueError("order payload outside the desk contract")
        return self._request("/v2/orders", method="POST", payload=payload)

    def order_by_client_id(self, client_order_id: str) -> dict[str, Any] | None:
        if not client_order_id:
            raise ValueError("client_order_id is required")
        escaped = urllib.parse.quote(client_order_id, safe="")
        try:
            return self._request("/v2/orders:by_client_order_id?client_order_id=" + escaped)
        except OrderNotFound:
            return None

    def cancel_order(self, broker_order_id: str) -> None:
        if not broker_order_id or "/" in broker_order_id:
            raise ValueError("invalid broker order id")
        self._request("/v2/orders/" + urllib.parse.quote(broker_order_id, safe=""), method="DELETE")
