"""Bounded, data-only Alpaca IEX/news WebSocket probe and offline replay.

Network access is opt-in (`probe --connect`). News bodies are never written.
This module does not import or call any brokerage/order API.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import queue
import re
import ssl
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
RUN_ROOT = DATA / "alpaca_live_feed_probes"
TEST_ROOT = DATA / "alpaca_ws_transport_tests"
PRICE_URL = "wss://stream.data.alpaca.markets/v2/iex"
NEWS_URL = "wss://stream.data.alpaca.markets/v1beta1/news"
TEST_URL = "wss://stream.data.alpaca.markets/v2/test"
MAX_DURATION_SECONDS = 60
MAX_TEST_DURATION_SECONDS = 10
MAX_PRICE_FRAMES = 10_000
MAX_NEWS_FRAMES = 2_000
MAX_RAW_BYTES = 4 * 1024 * 1024
MAX_FRAME_BYTES = 64 * 1024
QUEUE_CAPACITY = 128
MAX_STATUS_EVENTS = 256
MAX_NEWS_ARTICLES_PER_FRAME = 100
MAX_NEWS_TRACKED_IDS = 5_000
MAX_NEWS_SOURCE_KEYS = 32
MAX_CONNECTION_ATTEMPTS = 3  # initial connect + at most two reconnects
BACKOFF_SECONDS = (1, 2)
MAX_RUNS_STORAGE_BYTES = 100 * 1024 * 1024
RUN_RE = re.compile(r"^(?:probe|transport)_[0-9]{8}T[0-9]{6}Z_[0-9a-f]{8}$")


def utc_now_ns() -> int:
    return time.time_ns()


def utc_iso_ns(value: int) -> str:
    sec, ns = divmod(value, 1_000_000_000)
    stamp = datetime.fromtimestamp(sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{stamp}.{ns:09d}Z"


def parse_timestamp_ns(value: object) -> int | None:
    """Parse RFC3339 timestamps without losing sub-microsecond precision."""
    if not isinstance(value, str):
        return None
    m = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})",
        value,
    )
    if not m:
        return None
    year, month, day, hour, minute, second = map(int, m.groups()[:6])
    fraction = (m.group(7) or "").ljust(9, "0")
    zone = m.group(8)
    try:
        dt = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    except ValueError:
        return None
    if zone != "Z":
        sign = 1 if zone[0] == "+" else -1
        oh, om = map(int, zone[1:].split(":"))
        if oh > 23 or om > 59:
            return None
        offset_seconds = sign * (oh * 3600 + om * 60)
        dt = dt.replace(tzinfo=timezone.utc) - timedelta(seconds=offset_seconds)
    sec_ns = int(dt.timestamp()) * 1_000_000_000
    return sec_ns + int(fraction or "0")


def read_credentials(env_path: Path | None = None) -> tuple[str, str]:
    """Read credentials silently; environment takes precedence over ignored .env."""
    key = os.environ.get("APCA_API_KEY_ID", "")
    secret = os.environ.get("APCA_API_SECRET_KEY", "")
    if key and secret:
        return key, secret
    if env_path is None:
        env_path = ROOT / ".env"
    allowed = {"APCA_API_KEY_ID", "APCA_API_SECRET_KEY"}
    values: dict[str, str] = {}
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return key, secret
    for line in lines:
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        name, value = text.split("=", 1)
        if name.strip() in allowed:
            values[name.strip()] = value.strip().strip('"').strip("'")
    return key or values.get("APCA_API_KEY_ID", ""), secret or values.get("APCA_API_SECRET_KEY", "")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")


class Capture:
    def __init__(self, run_dir: Path, duration_seconds: int):
        self.run_dir = run_dir
        self.duration_seconds = duration_seconds
        self.started_utc_ns = utc_now_ns()
        self.started_monotonic_ns = time.monotonic_ns()
        self.deadline_ns = self.started_monotonic_ns + duration_seconds * 1_000_000_000
        self.stop_event = threading.Event()
        self.queue: queue.Queue[dict | None] = queue.Queue(maxsize=QUEUE_CAPACITY)
        self.lock = threading.RLock()
        self.apps: list[Any] = []
        self.global_sequence = 0
        self.endpoint_sequences = Counter()
        self.status_events: list[dict] = []
        self.status_drops = 0
        self.frame_counts = Counter()
        self.frame_type_counts: dict[str, Counter] = defaultdict(Counter)
        self.news_source_counts = Counter()
        self.news_seen: dict[int, int | None] = {}
        self.news_duplicate_count = 0
        self.news_update_count = 0
        self.news_article_count = 0
        self.news_created_min_ns: int | None = None
        self.news_created_max_ns: int | None = None
        self.news_updated_min_ns: int | None = None
        self.news_updated_max_ns: int | None = None
        self.news_received_min_ns: int | None = None
        self.news_received_max_ns: int | None = None
        self.news_publication_age_count = 0
        self.news_publication_age_min_ns: int | None = None
        self.news_publication_age_max_ns: int | None = None
        self.news_publication_age_negative_count = 0
        self.news_update_age_count = 0
        self.news_update_age_min_ns: int | None = None
        self.news_update_age_max_ns: int | None = None
        self.news_update_age_negative_count = 0
        self.oversize_frames = 0
        self.queue_drops = 0
        self.byte_budget_drops = 0
        self.message_budget_drops = 0
        self.writer_error_type: str | None = None
        self.raw_bytes_written = 0
        self.price_frames_written = 0
        self.writer_done = threading.Event()
        self._worker_done_by_endpoint: dict[str, threading.Event] = {}
        self.last_price_monotonic_ns: dict[str, int] = {}
        self.price_silence_notices: set[str] = set()
        self.connection_outcomes = Counter()
        self.closed_cleanly = False
        self.accept_frames = True
        self.late_frames_after_stop = 0

    def status(self, endpoint: str, event: str, *, attempt: int | None = None,
               code: int | None = None, detail: str | None = None) -> None:
        with self.lock:
            if len(self.status_events) >= MAX_STATUS_EVENTS:
                self.status_drops += 1
                return
            row: dict[str, object] = {
                "received_at_utc_ns": utc_now_ns(),
                "received_at_monotonic_ns": time.monotonic_ns(),
                "endpoint": endpoint,
                "event": event,
            }
            if attempt is not None:
                row["connection_attempt"] = attempt
            if code is not None and isinstance(code, int):
                row["code"] = code
            # `detail` is only supplied from a small local allowlist, never raw server text.
            if detail is not None:
                row["detail"] = detail
            self.status_events.append(row)
            self.connection_outcomes[event] += 1

    def submit_price_frame(self, endpoint: str, attempt: int, raw: str, parsed: list[dict]) -> None:
        raw_bytes = raw.encode("utf-8")
        with self.lock:
            if not self.accept_frames:
                self.late_frames_after_stop += 1
                return
            if len(raw_bytes) > MAX_FRAME_BYTES:
                self.oversize_frames += 1
                self.status(endpoint, "oversize_data_frame", attempt=attempt)
                return
            if self.frame_counts[endpoint] >= MAX_PRICE_FRAMES:
                self.message_budget_drops += 1
                self.stop_event.set()
                return
            self.frame_counts[endpoint] += 1
            self.last_price_monotonic_ns[endpoint] = time.monotonic_ns()
            for row in parsed:
                kind = row.get("T")
                if kind in ("q", "t"):
                    self.frame_type_counts[endpoint][kind] += 1
            # Sequence assignment and nonblocking queue insertion share one lock,
            # so persisted order exactly matches the deterministic replay order.
            self.global_sequence += 1
            self.endpoint_sequences[endpoint] += 1
            now_ns, mono_ns = utc_now_ns(), time.monotonic_ns()
            body = {
                "kind": "price_frame", "endpoint": endpoint,
                "received_at_utc_ns": now_ns, "received_at_utc": utc_iso_ns(now_ns),
                "received_at_monotonic_ns": mono_ns,
                "global_sequence": self.global_sequence, "frame_sequence": self.endpoint_sequences[endpoint],
                "connection_attempt": attempt,
                "event_types": sorted({x["T"] for x in parsed if x.get("T") in ("q", "t")}),
                "raw_frame_sha256": hashlib.sha256(raw_bytes).hexdigest(),
                "raw_frame_base64": base64.b64encode(raw_bytes).decode("ascii"),
            }
            try:
                self.queue.put_nowait(body)
            except queue.Full:
                self.queue_drops += 1

    def submit_news_frame(self, attempt: int, parsed: list[dict]) -> None:
        now_ns = utc_now_ns()
        articles = [x for x in parsed if x.get("T") == "n"]
        if len(articles) > MAX_NEWS_ARTICLES_PER_FRAME:
            with self.lock:
                self.oversize_frames += 1
            self.status("news", "article_count_cap_exceeded", attempt=attempt)
            return
        with self.lock:
            if not self.accept_frames:
                self.late_frames_after_stop += 1
                return
            if self.frame_counts["news"] >= MAX_NEWS_FRAMES:
                self.message_budget_drops += 1
                self.stop_event.set()
                return
            self.frame_counts["news"] += 1
            self.news_article_count += len(articles)
            for row in articles:
                source = row.get("source")
                if isinstance(source, str) and source.strip():
                    key = source[:80]
                    if key in self.news_source_counts or len(self.news_source_counts) < MAX_NEWS_SOURCE_KEYS - 1:
                        self.news_source_counts[key] += 1
                    else:
                        self.news_source_counts["<other_sources>"] += 1
                article_id = row.get("id")
                updated = row.get("updated_at")
                created_ns = parse_timestamp_ns(row.get("created_at"))
                updated_ns = parse_timestamp_ns(updated)
                if created_ns is not None:
                    self.news_created_min_ns = created_ns if self.news_created_min_ns is None else min(self.news_created_min_ns, created_ns)
                    self.news_created_max_ns = created_ns if self.news_created_max_ns is None else max(self.news_created_max_ns, created_ns)
                    age_ns = now_ns - created_ns
                    self.news_publication_age_count += 1
                    self.news_publication_age_negative_count += int(age_ns < 0)
                    self.news_publication_age_min_ns = age_ns if self.news_publication_age_min_ns is None else min(self.news_publication_age_min_ns, age_ns)
                    self.news_publication_age_max_ns = age_ns if self.news_publication_age_max_ns is None else max(self.news_publication_age_max_ns, age_ns)
                if updated_ns is not None:
                    self.news_updated_min_ns = updated_ns if self.news_updated_min_ns is None else min(self.news_updated_min_ns, updated_ns)
                    self.news_updated_max_ns = updated_ns if self.news_updated_max_ns is None else max(self.news_updated_max_ns, updated_ns)
                    age_ns = now_ns - updated_ns
                    self.news_update_age_count += 1
                    self.news_update_age_negative_count += int(age_ns < 0)
                    self.news_update_age_min_ns = age_ns if self.news_update_age_min_ns is None else min(self.news_update_age_min_ns, age_ns)
                    self.news_update_age_max_ns = age_ns if self.news_update_age_max_ns is None else max(self.news_update_age_max_ns, age_ns)
                self.news_received_min_ns = now_ns if self.news_received_min_ns is None else min(self.news_received_min_ns, now_ns)
                self.news_received_max_ns = now_ns if self.news_received_max_ns is None else max(self.news_received_max_ns, now_ns)
                if isinstance(article_id, int) and not isinstance(article_id, bool):
                    previous_exists = article_id in self.news_seen
                    previous = self.news_seen.get(article_id)
                    if previous_exists:
                        if previous == updated_ns:
                            self.news_duplicate_count += 1
                        else:
                            self.news_update_count += 1
                        self.news_seen[article_id] = updated_ns
                    elif len(self.news_seen) < MAX_NEWS_TRACKED_IDS:
                        self.news_seen[article_id] = updated_ns
        # Article content/headlines/URLs/symbols are intentionally not persisted or logged.

    def writer(self) -> None:
        path = self.run_dir / "price_frames.jsonl"
        try:
            with path.open("xb") as stream:
                try:
                    path.chmod(0o600)
                except OSError:
                    pass
                while True:
                    try:
                        item = self.queue.get(timeout=0.2)
                    except queue.Empty:
                        if self.stop_event.is_set() and all(t.is_set() for t in self._worker_done):
                            break
                        continue
                    if item is None:
                        self.queue.task_done()
                        break
                    line = _json_bytes(item)
                    if self.raw_bytes_written + len(line) > MAX_RAW_BYTES:
                        with self.lock:
                            self.byte_budget_drops += 1
                        self.stop_event.set()
                    else:
                        stream.write(line)
                        self.raw_bytes_written += len(line)
                        self.price_frames_written += 1
                    self.queue.task_done()
                stream.flush()
                os.fsync(stream.fileno())
        except Exception as exc:  # persist safe type only
            self.writer_error_type = type(exc).__name__
            self.stop_event.set()
        finally:
            self.writer_done.set()

    def _worker_finished(self, endpoint: str) -> None:
        self._worker_done_by_endpoint[endpoint].set()

    @property
    def _worker_done(self) -> list[threading.Event]:
        return list(self._worker_done_by_endpoint.values())

    def close_apps(self) -> None:
        with self.lock:
            apps = list(self.apps)
        for app in apps:
            try:
                app.close()
            except Exception:
                pass

    def add_app(self, app: Any) -> None:
        with self.lock:
            self.apps.append(app)


def _control_rows(raw: str | bytes) -> tuple[list[dict], str]:
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="strict")
    else:
        text = raw
    value = json.loads(text)
    if isinstance(value, dict):
        rows = [value]
    elif isinstance(value, list):
        rows = [x for x in value if isinstance(x, dict)]
    else:
        rows = []
    return rows, text


def _safe_code(row: dict) -> int | None:
    code = row.get("code")
    return code if isinstance(code, int) and not isinstance(code, bool) else None


def _event_type_counts(parsed: list[dict]) -> Counter:
    return Counter(str(row.get("T")) for row in parsed if isinstance(row.get("T"), str))


def run_endpoint(capture: Capture, endpoint: str, url: str, key: str, secret: str,
                 subscriptions: dict[str, list[str]]) -> None:
    """Run one endpoint with bounded reconnects using websocket-client only."""
    import websocket  # imported only for explicit network mode
    websocket.enableTrace(False)
    logging.getLogger("websocket").disabled = True

    done = threading.Event()
    capture._worker_done_by_endpoint[endpoint] = done
    attempts_limit = MAX_CONNECTION_ATTEMPTS
    for attempt in range(1, attempts_limit + 1):
        if capture.stop_event.is_set() or time.monotonic_ns() >= capture.deadline_ns:
            break
        authenticated = threading.Event()
        subscribed = threading.Event()
        auth_rejected = threading.Event()
        terminal_close = threading.Event()
        attempt_state = {"opened": False, "sent_auth": False, "sent_subscribe": False}

        def on_open(app: Any) -> None:
            attempt_state["opened"] = True
            capture.status(endpoint, "connected", attempt=attempt)

        def handle_message(app: Any, message: str | bytes) -> None:
            try:
                message_size = len(message if isinstance(message, bytes) else message.encode("utf-8"))
            except Exception as exc:
                capture.status(endpoint, "malformed_message", attempt=attempt,
                               detail=type(exc).__name__[:48])
                return
            if message_size > MAX_FRAME_BYTES:
                with capture.lock:
                    capture.oversize_frames += 1
                capture.status(endpoint, "oversize_frame_dropped", attempt=attempt)
                return
            try:
                rows, text = _control_rows(message)
            except Exception as exc:
                capture.status(endpoint, "malformed_message", attempt=attempt,
                               detail=type(exc).__name__[:48])
                return
            counts = _event_type_counts(rows)
            control_rows = [row for row in rows if row.get("T") in ("success", "error", "subscription")]
            if control_rows:
                for row in control_rows:
                    typ = row.get("T")
                    if typ == "success":
                        msg = row.get("msg")
                        if msg == "connected" and not attempt_state["sent_auth"]:
                            try:
                                app.send(json.dumps({"action": "auth", "key": key, "secret": secret}, separators=(",", ":")))
                                attempt_state["sent_auth"] = True
                                capture.status(endpoint, "auth_sent", attempt=attempt)
                            except Exception as exc:
                                capture.status(endpoint, "send_error", attempt=attempt,
                                               detail=type(exc).__name__[:48])
                                auth_rejected.set()
                                app.close()
                        elif msg == "authenticated":
                            authenticated.set()
                            capture.status(endpoint, "authenticated", attempt=attempt)
                            try:
                                app.send(json.dumps({"action": "subscribe", **subscriptions}, separators=(",", ":")))
                                attempt_state["sent_subscribe"] = True
                                capture.status(endpoint, "subscribe_sent", attempt=attempt)
                            except Exception as exc:
                                capture.status(endpoint, "send_error", attempt=attempt,
                                               detail=type(exc).__name__[:48])
                                auth_rejected.set()
                                app.close()
                        else:
                            capture.status(endpoint, "success_control", attempt=attempt)
                    elif typ == "subscription":
                        safe_channels = sorted(k for k, v in row.items()
                                               if k in ("quotes", "trades", "news") and isinstance(v, list))
                        exact_ack = all(isinstance(row.get(channel), list) and
                                        all(isinstance(item, str) for item in row[channel]) and
                                        set(wanted) == set(row[channel])
                                        for channel, wanted in subscriptions.items())
                        if exact_ack:
                            subscribed.set()
                            capture.status(endpoint, "subscription_ack", attempt=attempt,
                                           detail="channels_" + "_".join(safe_channels or ["none"]))
                        else:
                            capture.status(endpoint, "subscription_ack_mismatch", attempt=attempt)
                            auth_rejected.set()
                            app.close()
                    elif typ == "error":
                        code = _safe_code(row)
                        # Server-controlled text is intentionally discarded.
                        capture.status(endpoint, "server_error", attempt=attempt, code=code)
                        if code in (401, 402, 405, 406, 409, 410):
                            auth_rejected.set()
                            app.close()
            data_rows = [row for row in rows if row.get("T") not in ("success", "error", "subscription")]
            if not data_rows:
                return
            if endpoint == "news":
                capture.submit_news_frame(attempt, data_rows)
            elif any(row.get("T") in ("q", "t") for row in data_rows):
                # Persist only price frames; any server control records are excluded.
                frame_text = text if len(data_rows) == len(rows) else json.dumps(
                    data_rows, separators=(",", ":"), ensure_ascii=False)
                capture.submit_price_frame(endpoint, attempt, frame_text, data_rows)
            else:
                # Ignore non-price rows and never persist arbitrary server messages.
                capture.status(endpoint, "non_price_frame_ignored", attempt=attempt)

        def on_message(app: Any, message: str | bytes) -> None:
            try:
                handle_message(app, message)
            except Exception as exc:
                capture.status(endpoint, "callback_exception", attempt=attempt,
                               detail=type(exc).__name__[:48])

        def on_error(app: Any, error: Exception) -> None:
            code = _safe_code(getattr(error, "args", [{}])[0]) if error.args and isinstance(error.args[0], dict) else None
            status_code = getattr(error, "status_code", None)
            capture.status(endpoint, "socket_error", attempt=attempt,
                           code=status_code if isinstance(status_code, int) else code,
                           detail=type(error).__name__[:48])

        def on_close(app: Any, close_status: int | None, close_message: str | None) -> None:
            # Do not persist the close reason: it may contain server-controlled text.
            early = not capture.stop_event.is_set() and time.monotonic_ns() < capture.deadline_ns
            capture.status(endpoint, "unexpected_close" if early else "closed", attempt=attempt,
                           code=close_status if isinstance(close_status, int) else None)
            terminal_close.set()

        app = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        capture.add_app(app)
        try:
            app.run_forever(ping_interval=20, ping_timeout=10,
                            sslopt={"cert_reqs": ssl.CERT_REQUIRED})
        except Exception as exc:
            capture.status(endpoint, "run_error", attempt=attempt,
                           detail=type(exc).__name__[:48])
        if capture.stop_event.is_set() or time.monotonic_ns() >= capture.deadline_ns:
            break
        if auth_rejected.is_set():
            capture.status(endpoint, "reconnect_stopped_auth_or_subscription_error", attempt=attempt)
            break
        if not terminal_close.is_set() and not attempt_state["opened"]:
            capture.status(endpoint, "connection_not_established", attempt=attempt)
        if attempt < attempts_limit:
            delay = BACKOFF_SECONDS[attempt - 1]
            if time.monotonic_ns() + delay * 1_000_000_000 >= capture.deadline_ns:
                break
            capture.status(endpoint, "reconnect_backoff", attempt=attempt,
                           detail=f"seconds_{delay}")
            capture.stop_event.wait(delay)
    done.set()


def config_default() -> dict:
    return {
        "schema_version": 1,
        "continuous_collection_enabled": False,
        "feed": "iex",
        "symbols": ["SPY"],
        "probe_duration_seconds": MAX_DURATION_SECONDS,
        "max_raw_bytes_per_run": MAX_RAW_BYTES,
        "max_total_storage_bytes": MAX_RUNS_STORAGE_BYTES,
        "max_price_frames": MAX_PRICE_FRAMES,
        "max_news_frames": MAX_NEWS_FRAMES,
        "max_frame_bytes": MAX_FRAME_BYTES,
        "queue_capacity": QUEUE_CAPACITY,
        "reconnect_attempts": MAX_CONNECTION_ATTEMPTS,
        "reconnect_backoff_seconds": [1, 2],
        "news_body_persistence": False,
        "market_hours_guard_enabled": False
    }


def load_config(path: Path) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    defaults = config_default()
    if not isinstance(cfg, dict) or cfg != {**defaults, **cfg}:
        # Reject unknown keys but permit each explicit value to differ within bounds below.
        raise ValueError("config schema/keys do not match approved collector fields")
    expected = set(defaults)
    if set(cfg) != expected or cfg.get("schema_version") != 1:
        raise ValueError("collector config schema mismatch")
    if cfg.get("feed") != "iex" or cfg.get("symbols") != ["SPY"]:
        raise ValueError("probe allowlist is fixed to SPY on IEX")
    if cfg.get("continuous_collection_enabled") is not False:
        raise ValueError("continuous collection remains disabled in this revision")
    if cfg.get("news_body_persistence") is not False:
        raise ValueError("news body storage is prohibited by this collector revision")
    if cfg.get("probe_duration_seconds") != MAX_DURATION_SECONDS:
        raise ValueError("first probe duration is fixed at 60 seconds")
    if cfg.get("max_raw_bytes_per_run") != MAX_RAW_BYTES or cfg.get("max_frame_bytes") != MAX_FRAME_BYTES:
        raise ValueError("raw capture bounds are immutable in this revision")
    if cfg.get("max_price_frames") != MAX_PRICE_FRAMES or cfg.get("max_news_frames") != MAX_NEWS_FRAMES:
        raise ValueError("message bounds are immutable in this revision")
    if cfg.get("queue_capacity") != QUEUE_CAPACITY:
        raise ValueError("writer queue bound is immutable in this revision")
    if cfg.get("reconnect_attempts") != MAX_CONNECTION_ATTEMPTS or cfg.get("reconnect_backoff_seconds") != list(BACKOFF_SECONDS):
        raise ValueError("reconnect policy mismatch")
    if cfg.get("max_total_storage_bytes", 0) < MAX_RAW_BYTES:
        raise ValueError("total storage cap must be at least one bounded raw capture")
    return cfg


def _folder_bytes(path: Path) -> int:
    total = 0
    if path.exists():
        for item in path.rglob("*"):
            if item.is_file() and not item.is_symlink():
                total += item.stat().st_size
    return total


def endpoint_outcomes(status_events: list[dict]) -> dict[str, dict]:
    result = {}
    for endpoint in ("iex", "news", "test"):
        events = [row.get("event") for row in status_events if row.get("endpoint") == endpoint]
        if not events:
            continue
        authenticated = "authenticated" in events
        subscribed = "subscription_ack" in events
        rejected = any(name in events for name in ("server_error", "subscription_ack_mismatch",
                                                    "reconnect_stopped_auth_or_subscription_error"))
        state = "rejected_or_mismatched" if rejected else (
            "subscription_acknowledged" if authenticated and subscribed else "not_confirmed")
        result[endpoint] = {"authenticated": authenticated,
                            "exact_subscription_acknowledged": subscribed,
                            "rejected_or_mismatched": rejected,
                            "state": state}
    return result


def capture_discontinuities(status_events: list[dict]) -> dict[str, int]:
    relevant = {"socket_error", "run_error", "send_error", "unexpected_close",
                "connection_not_established", "reconnect_backoff", "server_error",
                "subscription_ack_mismatch", "worker_shutdown_timeout", "worker_exception", "callback_exception",
                "price_silence_over_5s", "oversize_frame_dropped",
                "oversize_data_frame", "article_count_cap_exceeded"}
    return dict(sorted(Counter(row.get("event") for row in status_events
                               if row.get("event") in relevant).items()))


def new_run_dir(root: Path = RUN_ROOT, prefix: str = "probe") -> tuple[str, Path]:
    root.mkdir(parents=True, exist_ok=True)
    resolved_root = root.resolve(strict=True)
    if not resolved_root.is_relative_to(DATA.resolve(strict=True)):
        raise ValueError("collector output path escapes ignored data root")
    run_id = prefix + "_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + os.urandom(4).hex()
    path = root / run_id
    path.mkdir(exist_ok=False)
    try:
        path.chmod(0o700)
    except OSError:
        pass
    return run_id, path


def _thread_capture(capture: Capture, key: str, secret: str,
                    streams: list[tuple[str, str, dict[str, list[str]]]] | None = None) -> None:
    if streams is None:
        streams = [
            ("iex", PRICE_URL, {"quotes": ["SPY"], "trades": ["SPY"]}),
            ("news", NEWS_URL, {"news": ["*"]}),
        ]
    # Workers execute concurrently; the two production endpoints have independent connections.
    threads = []
    for endpoint, url, sub in streams:
        def safe_worker(endpoint_name: str = endpoint, endpoint_url: str = url,
                        requested_channels: dict[str, list[str]] = sub) -> None:
            try:
                run_endpoint(capture, endpoint_name, endpoint_url, key, secret, requested_channels)
            except Exception as exc:
                capture.status(endpoint_name, "worker_exception", detail=type(exc).__name__[:48])
                done_event = capture._worker_done_by_endpoint.get(endpoint_name)
                if done_event is not None:
                    done_event.set()

        t = threading.Thread(target=safe_worker, daemon=True)
        threads.append(t)
    capture._worker_done_by_endpoint = {name: threading.Event() for name, _, _ in streams}
    writer_thread = threading.Thread(target=capture.writer, name="price-writer", daemon=True)
    writer_thread.start()
    for t in threads:
        t.start()
    deadline = time.monotonic() + capture.duration_seconds
    while time.monotonic() < deadline and not capture.stop_event.is_set():
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
        # Track slow/no-data periods for price socket. Silence is recorded, never treated as proof of outage.
        now = time.monotonic_ns()
        with capture.lock:
            for endpoint, last_ns in list(capture.last_price_monotonic_ns.items()):
                if now - last_ns >= 5_000_000_000 and endpoint not in capture.price_silence_notices:
                    capture.price_silence_notices.add(endpoint)
                    capture.status(endpoint, "price_silence_over_5s")
    capture.stop_event.set()
    capture.close_apps()
    for t in threads:
        t.join(timeout=5)
    capture.accept_frames = False
    capture.workers_stopped = all(not t.is_alive() for t in threads)
    if not capture.workers_stopped:
        capture.status("collector", "worker_shutdown_timeout")
        capture.close_apps()
        for done in capture._worker_done_by_endpoint.values():
            done.set()
    writer_thread.join(timeout=10)
    capture.closed_cleanly = capture.writer_done.is_set() and writer_thread.is_alive() is False and capture.workers_stopped


def run_probe(*, connect: bool, config_path: Path) -> dict:
    cfg = load_config(config_path)
    if not connect:
        return {"mode": "dry_run_no_connection", "endpoints": [PRICE_URL, NEWS_URL],
                "symbol": "SPY", "feed": "IEX", "duration_seconds": cfg["probe_duration_seconds"],
                "news_content_persisted": False,
                "continuous_collection_enabled": False, "no_broker_or_orders": True}
    if _folder_bytes(RUN_ROOT) + MAX_RAW_BYTES + 256 * 1024 > cfg["max_total_storage_bytes"]:
        raise RuntimeError("collector storage cap reached; evidence is preserved and no deletion is automatic")
    key, secret = read_credentials()
    if not key or not secret:
        raise ValueError("credentials are not configured; no connection attempted")
    run_id, run_dir = new_run_dir()
    capture = Capture(run_dir, cfg["probe_duration_seconds"])
    # Never keep credentials in the capture object or manifest.
    try:
        _thread_capture(capture, key, secret)
    finally:
        key = ""
        secret = ""
    raw_path = run_dir / "price_frames.jsonl"
    raw_bytes = raw_path.read_bytes() if raw_path.is_file() else b""
    outcomes = endpoint_outcomes(capture.status_events)
    discontinuities = capture_discontinuities(capture.status_events)
    has_drops = bool(capture.queue_drops or capture.byte_budget_drops or
                     capture.message_budget_drops or capture.writer_error_type or
                     capture.oversize_frames or capture.status_drops or not capture.closed_cleanly)
    subscribed = all(outcomes.get(ep, {}).get("state") == "subscription_acknowledged"
                     for ep in ("iex", "news"))
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "started_at_utc": utc_iso_ns(capture.started_utc_ns),
        "started_at_monotonic_ns": capture.started_monotonic_ns,
        "finished_at_utc": utc_iso_ns(utc_now_ns()),
        "actual_duration_monotonic_ns": max(0, time.monotonic_ns() - capture.started_monotonic_ns),
        "requested_duration_seconds": cfg["probe_duration_seconds"],
        "actual_raw_bytes": len(raw_bytes),
        "raw_file": "price_frames.jsonl",
        "raw_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "persisted_price_frame_count": capture.price_frames_written,
        "total_received_price_and_news_frames": sum(capture.frame_counts.values()),
        "frame_counts_by_endpoint": dict(sorted(capture.frame_counts.items())),
        "event_type_counts_by_endpoint": {k: dict(sorted(v.items())) for k, v in sorted(capture.frame_type_counts.items())},
        "news": {
            "frames": capture.frame_counts["news"],
            "article_count": capture.news_article_count,
            "source_counts": dict(sorted(capture.news_source_counts.items())),
            "article_duplicates_same_updated_at": capture.news_duplicate_count,
            "article_updates_same_id_new_updated_at": capture.news_update_count,
            "created_at_min_utc_ns": capture.news_created_min_ns,
            "created_at_max_utc_ns": capture.news_created_max_ns,
            "updated_at_min_utc_ns": capture.news_updated_min_ns,
            "updated_at_max_utc_ns": capture.news_updated_max_ns,
            "received_at_min_utc_ns": capture.news_received_min_ns,
            "received_at_max_utc_ns": capture.news_received_max_ns,
            "publication_age_ns": {"count": capture.news_publication_age_count,
                                    "minimum": capture.news_publication_age_min_ns,
                                    "maximum": capture.news_publication_age_max_ns,
                                    "negative_clock_count": capture.news_publication_age_negative_count},
            "updated_timestamp_age_ns": {"count": capture.news_update_age_count,
                                          "minimum": capture.news_update_age_min_ns,
                                          "maximum": capture.news_update_age_max_ns,
                                          "negative_clock_count": capture.news_update_age_negative_count},
            "age_semantics": "local receipt clock minus vendor created_at/updated_at; not network or provider transport latency",
            "article_ids_or_content_persisted": False,
            "headlines_bodies_urls_or_ticker_lists_persisted": False,
        },
        "limits": {"raw_bytes": MAX_RAW_BYTES, "frame_bytes": MAX_FRAME_BYTES,
                   "price_frames": MAX_PRICE_FRAMES, "news_frames": MAX_NEWS_FRAMES,
                   "writer_queue_items": QUEUE_CAPACITY,
                   "connection_attempts_per_endpoint": MAX_CONNECTION_ATTEMPTS,
                   "reconnect_backoff_seconds": list(BACKOFF_SECONDS)},
        "drops": {"writer_queue": capture.queue_drops, "byte_budget": capture.byte_budget_drops,
                  "message_budget": capture.message_budget_drops,
                  "oversize_frames": capture.oversize_frames, "status_events": capture.status_drops,
                  "late_after_shutdown": capture.late_frames_after_stop},
        "writer_error_type": capture.writer_error_type,
        "status_events": capture.status_events,
        "status_event_count": len(capture.status_events),
        "endpoint_outcomes": outcomes,
        "capture_discontinuities": discontinuities,
        "continuous_capture_claimed": False,
        "feed_freshness_established": False,
        "closed_cleanly": capture.closed_cleanly,
        "feed": "IEX single-exchange, non-consolidated",
        "endpoints": {"price": PRICE_URL, "news": NEWS_URL},
        "probe_only": True,
        "holdout_or_2026_history_used": False,
        "no_sip_or_historical_requests": True,
        "news_entitlement_verified": False,
        "no_signal_or_performance_calculated": True,
        "no_broker_or_orders": True,
        "raw_data_private_research_only": True,
        "news_socket_status": outcomes.get("news", {}).get("state", "not_confirmed"),
        "run_status": "incomplete" if has_drops or capture.late_frames_after_stop else (
            "review_required_connection_discontinuity" if discontinuities else (
                "bounded_capture_complete" if subscribed else "review_required_auth_or_subscription_unconfirmed")),
        "freshness_status": "unknown_no_messages" if not any(capture.frame_counts.values()) else "not_independently_verified",
        "interpretation": "A quiet after-hours window or no news articles is inconclusive for realtime freshness/entitlement.",
    }
    manifest_path = run_dir / "manifest.json"
    with manifest_path.open("x", encoding="utf-8") as stream:
        try:
            manifest_path.chmod(0o600)
        except OSError:
            pass
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    return {"run_id": run_id, "run_dir": str(run_dir), "run_status": manifest["run_status"],
            "price_frames": manifest["frame_counts_by_endpoint"].get("iex", 0),
            "news_frames": manifest["news"]["frames"], "raw_bytes": len(raw_bytes),
            "endpoint_outcomes": outcomes, "news_content_persisted": False, "secrets_printed": False}


def run_transport_test(*, connect: bool) -> dict:
    """Optional 10-second synthetic transport check, stored apart from market capture."""
    if not connect:
        return {"mode": "dry_run_no_connection", "endpoint": TEST_URL,
                "symbol": "FAKEPACA", "duration_seconds": MAX_TEST_DURATION_SECONDS,
                "synthetic_transport_only": True, "market_data_capture": False,
                "news_or_outcomes": False}
    if _folder_bytes(TEST_ROOT) + MAX_RAW_BYTES + 256 * 1024 > 2 * MAX_RAW_BYTES:
        raise RuntimeError("synthetic transport-test storage cap reached; no evidence will be deleted")
    key, secret = read_credentials()
    if not key or not secret:
        raise ValueError("credentials are not configured; no connection attempted")
    run_id, run_dir = new_run_dir(TEST_ROOT, "transport")
    capture = Capture(run_dir, MAX_TEST_DURATION_SECONDS)
    try:
        _thread_capture(capture, key, secret, streams=[("test", TEST_URL, {"trades": ["FAKEPACA"]})])
    finally:
        key = ""
        secret = ""
    raw_path = run_dir / "price_frames.jsonl"
    raw_bytes = raw_path.read_bytes() if raw_path.is_file() else b""
    outcomes = endpoint_outcomes(capture.status_events)
    has_drops = bool(capture.queue_drops or capture.byte_budget_drops or capture.message_budget_drops or
                     capture.writer_error_type or capture.oversize_frames or capture.status_drops or
                     not capture.closed_cleanly)
    outcome = outcomes.get("test", {}).get("state") == "subscription_acknowledged"
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "kind": "synthetic_transport_test_only",
        "started_at_utc": utc_iso_ns(capture.started_utc_ns),
        "started_at_monotonic_ns": capture.started_monotonic_ns,
        "finished_at_utc": utc_iso_ns(utc_now_ns()),
        "actual_duration_monotonic_ns": max(0, time.monotonic_ns() - capture.started_monotonic_ns),
        "requested_duration_seconds": MAX_TEST_DURATION_SECONDS,
        "symbol": "FAKEPACA",
        "endpoint": TEST_URL,
        "raw_file": "price_frames.jsonl",
        "actual_raw_bytes": len(raw_bytes),
        "raw_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "frame_count": capture.frame_counts["test"],
        "persisted_synthetic_test_frame_count": capture.price_frames_written,
        "event_type_counts": dict(sorted(capture.frame_type_counts["test"].items())),
        "endpoint_outcomes": outcomes,
        "capture_discontinuities": capture_discontinuities(capture.status_events),
        "drops": {"writer_queue": capture.queue_drops, "byte_budget": capture.byte_budget_drops,
                  "message_budget": capture.message_budget_drops, "oversize_frames": capture.oversize_frames,
                  "status_events": capture.status_drops, "late_after_shutdown": capture.late_frames_after_stop},
        "status_events": capture.status_events,
        "writer_error_type": capture.writer_error_type,
        "closed_cleanly": capture.closed_cleanly,
        "market_data_capture": False,
        "synthetic_transport_only": True,
        "no_news_or_signal_or_performance": True,
        "no_broker_or_orders": True,
        "run_status": "incomplete" if has_drops or capture.late_frames_after_stop else (
            "review_required_connection_discontinuity" if capture_discontinuities(capture.status_events) else (
                "bounded_transport_test_complete" if outcome else "review_required_auth_or_subscription_unconfirmed")),
    }
    manifest_path = run_dir / "manifest.json"
    with manifest_path.open("x", encoding="utf-8") as stream:
        try:
            manifest_path.chmod(0o600)
        except OSError:
            pass
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    return {"run_id": run_id, "run_dir": str(run_dir), "run_status": manifest["run_status"],
            "synthetic_transport_only": True, "market_data_capture": False,
            "test_frames": manifest["frame_count"], "raw_bytes": len(raw_bytes),
            "endpoint_outcomes": outcomes}


def replay_run(run_name: str) -> dict:
    if not RUN_RE.fullmatch(run_name):
        raise ValueError("pass a bounded probe directory name, not an arbitrary path")
    root = TEST_ROOT if run_name.startswith("transport_") else RUN_ROOT
    run_dir = root / run_name
    root_resolved, dir_resolved = root.resolve(strict=True), run_dir.resolve(strict=True)
    if not dir_resolved.is_relative_to(root_resolved):
        raise ValueError("run path escapes collector data root")
    manifest_path = run_dir / "manifest.json"
    if manifest_path.is_symlink() or manifest_path.stat().st_size > 256 * 1024:
        raise ValueError("unsafe or oversized probe manifest")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("run_id") != run_name:
        raise ValueError("manifest identity does not match requested probe directory")
    raw_name = manifest.get("raw_file")
    if raw_name != "price_frames.jsonl":
        raise ValueError("unexpected raw filename")
    raw_path = run_dir / raw_name
    if raw_path.is_symlink() or raw_path.stat().st_size > MAX_RAW_BYTES:
        raise ValueError("unsafe or oversized raw price file")
    raw = raw_path.read_bytes()
    if len(raw) != manifest.get("actual_raw_bytes") or hashlib.sha256(raw).hexdigest() != manifest.get("raw_sha256"):
        raise ValueError("raw price file hash/length mismatch")
    if len(raw) > MAX_RAW_BYTES:
        raise ValueError("raw price file exceeds the fixed replay bound")
    seen = set()
    counts = Counter()
    duplicate_frames = 0
    future_events = 0
    out_of_order_event_times = 0
    stale_events = 0
    last_event_ns: dict[str, int] = {}
    last_received_ns: dict[str, int] = {}
    receive_gaps: dict[str, list[int]] = defaultdict(list)
    replayed_sequence = 0
    record_sequence_gaps = 0
    synthetic_transport = manifest.get("kind") == "synthetic_transport_test_only"
    expected_endpoint = "test" if synthetic_transport else "iex"
    for lineno, line in enumerate(raw.splitlines(), 1):
        row = json.loads(line)
        if row.get("kind") != "price_frame" or not isinstance(row.get("global_sequence"), int) or isinstance(row.get("global_sequence"), bool):
            raise ValueError(f"invalid price frame record at line {lineno}")
        if row["global_sequence"] <= replayed_sequence:
            raise ValueError("record order is not strictly increasing")
        if not replayed_sequence and row["global_sequence"] > 1:
            record_sequence_gaps += row["global_sequence"] - 1
        elif replayed_sequence and row["global_sequence"] > replayed_sequence + 1:
            record_sequence_gaps += row["global_sequence"] - replayed_sequence - 1
        replayed_sequence = row["global_sequence"]
        endpoint = row.get("endpoint")
        if endpoint != expected_endpoint:
            raise ValueError("unknown raw endpoint")
        try:
            frame = base64.b64decode(row["raw_frame_base64"], validate=True)
        except Exception as exc:
            raise ValueError("invalid raw frame encoding") from exc
        digest = hashlib.sha256(frame).hexdigest()
        if digest != row.get("raw_frame_sha256"):
            raise ValueError("individual raw frame hash mismatch")
        counts["test_frames" if synthetic_transport else "frames"] += 1
        if digest in seen:
            duplicate_frames += 1
        seen.add(digest)
        received_ns = row.get("received_at_utc_ns")
        if not isinstance(received_ns, int) or isinstance(received_ns, bool):
            raise ValueError("missing receive UTC nanoseconds")
        if endpoint in last_received_ns:
            receive_gaps[endpoint].append(received_ns - last_received_ns[endpoint])
        last_received_ns[endpoint] = received_ns
        payload = json.loads(frame)
        data_rows = [payload] if isinstance(payload, dict) else [x for x in payload if isinstance(x, dict)]
        for event in data_rows:
            kind = event.get("T")
            if kind not in ("q", "t"):
                continue
            counts["quotes" if kind == "q" else "trades"] += 1
            event_ns = parse_timestamp_ns(event.get("t"))
            if event_ns is None:
                counts["invalid_event_timestamp"] += 1
                continue
            if event_ns > received_ns:
                future_events += 1
                continue  # never make future event data available to this replay state
            if endpoint in last_event_ns and event_ns < last_event_ns[endpoint]:
                out_of_order_event_times += 1
            last_event_ns[endpoint] = event_ns
            if received_ns - event_ns > 5_000_000_000:
                stale_events += 1
    return {
        "mode": "offline_receipt_order_replay",
        "run_id": manifest.get("run_id"),
        "raw_hash_verified": True,
        "synthetic_transport_only": synthetic_transport,
        "market_data_capture": not synthetic_transport,
        "replay_order": "global_sequence (callback receipt order); no event-time sorting or future-frame lookahead",
        "price_frames": counts["frames"], "quotes": counts["quotes"], "trades": counts["trades"],
        "synthetic_test_frames": counts["test_frames"],
        "exact_duplicate_frames": duplicate_frames,
        "record_sequence_gaps": record_sequence_gaps,
        "future_event_timestamps_excluded": future_events,
        "out_of_order_event_timestamps": out_of_order_event_times,
        "event_lag_over_5s": stale_events,
        "invalid_event_timestamps": counts["invalid_event_timestamp"],
        "receive_gaps_by_endpoint_ns": {k: v for k, v in sorted(receive_gaps.items())},
        "news_replay": "unavailable; news payloads were not retained",
        "signal_or_performance_calculated": False,
        "future_updates_used": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("probe", help="dry-run by default; connect only with explicit --connect")
    p.add_argument("--config", type=Path, default=ROOT / "config" / "live_collector.json")
    p.add_argument("--connect", action="store_true")
    t = sub.add_parser("test-transport", help="optional synthetic FAKEPACA transport check; dry-run by default")
    t.add_argument("--connect", action="store_true")
    r = sub.add_parser("replay", help="verify and replay an existing private probe offline")
    r.add_argument("run_id")
    args = parser.parse_args()
    if args.command == "probe":
        result = run_probe(connect=args.connect, config_path=args.config)
    elif args.command == "test-transport":
        result = run_transport_test(connect=args.connect)
    else:
        result = replay_run(args.run_id)
    print(json.dumps(result, indent=2))
    if args.command == "probe" and not args.connect:
        return 0
    if args.command == "test-transport" and not args.connect:
        return 0
    if args.command == "replay":
        return 0
    return 0 if result.get("run_status") in ("bounded_capture_complete", "bounded_transport_test_complete") else 1


if __name__ == "__main__":
    raise SystemExit(main())
