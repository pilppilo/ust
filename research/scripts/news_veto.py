"""News and event veto (build item C4). Veto-only by recorded decision (M2 spec 5.2; operator 2026-11-12).

A pure reader of two small local files; it reads no broker and no network, places nothing and
imports nothing from the engine. It can only turn an entry into an abstention. There is no
code path here that creates, enlarges or re-prices an order, and ``news_gate`` returns a
reason string or None, never a quantity or a side.

Inputs
  headline metadata  JSONL, one object per line: ``{"type": "headline", "id", "created_at",
                     "received_at", "symbols": [...], "source"}`` and heartbeats
                     ``{"type": "heartbeat", "at": ...}``. No headline text, no body, no URL is
                     read or kept (a line carrying a ``headline``/``content``/``summary``/``url``
                     key is refused as a whole, so text never enters a decision record).
  event calendar     JSON ``{"events": [{"name", "start_utc", "end_utc", "symbols": [..]|"all"}]}``
                     (FOMC, CPI, NFP, early closes and the like, from public sources, S2-D).

Rules (all thresholds are data, unvalidated starting policy; a frozen study (S5) decides whether
a veto helps)
  * a headline tagged with the symbol, received within ``lookback_minutes`` of now: veto
  * an event window naming the symbol (or "all") that contains now, widened by
    ``event_pad_minutes`` each side: veto
  * ``required`` and the metadata stream has no heartbeat within ``max_heartbeat_age_seconds``
    (or the file is missing/unreadable): veto ``news_feed_unavailable`` (fail closed). When not
    required, an unreadable feed is recorded and does not veto.
Only the tail of the file (``tail_bytes``) is read, so a long day costs the same per cycle.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

SCHEMA = "news-veto-v1"
TEXT_KEYS = frozenset({"headline", "content", "summary", "url", "body", "title", "images"})
DEFAULTS = {"lookback_minutes": 15.0, "event_pad_minutes": 10.0, "max_heartbeat_age_seconds": 180.0,
            "tail_bytes": 262144, "required": False}


class NewsVetoError(Exception):
    pass


def parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        # nanosecond stamps: keep microseconds
        head, _, rest = text.partition(".")
        frac = "".join(c for c in rest if c.isdigit())[:6]
        tz = rest[len(rest.rstrip("0123456789+-:")):] if rest else ""
        try:
            parsed = datetime.fromisoformat(f"{head}.{frac.ljust(6, '0')}{tz}")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _tail_lines(path: Path, tail_bytes: int) -> list[str]:
    size = path.stat().st_size
    with path.open("rb") as handle:
        if size > tail_bytes:
            handle.seek(size - tail_bytes)
            handle.readline()  # drop the partial first line
        data = handle.read()
    return [ln for ln in data.decode("utf-8", errors="replace").splitlines() if ln.strip()]


def load_headlines(path: str | Path, *, tail_bytes: int = DEFAULTS["tail_bytes"]):
    """(headlines, last_heartbeat, refused_lines). Raises OSError when the file is unreadable."""
    headlines, heartbeat, refused = [], None, 0
    for line in _tail_lines(Path(path), int(tail_bytes)):
        try:
            row = json.loads(line)
        except ValueError:
            refused += 1
            continue
        if not isinstance(row, dict) or TEXT_KEYS & row.keys():
            refused += 1
            continue
        kind = row.get("type")
        if kind == "heartbeat":
            at = parse_utc(row.get("at"))
            if at is not None and (heartbeat is None or at > heartbeat):
                heartbeat = at
        elif kind == "headline":
            received = parse_utc(row.get("received_at")) or parse_utc(row.get("created_at"))
            symbols = row.get("symbols")
            if received is None or not isinstance(symbols, list):
                refused += 1
                continue
            headlines.append({"id": row.get("id"), "received": received,
                              "symbols": {str(s).upper() for s in symbols}})
    return headlines, heartbeat, refused


def load_events(path: str | Path) -> list[dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    out = []
    for event in data.get("events") or []:
        start, end = parse_utc(event.get("start_utc")), parse_utc(event.get("end_utc"))
        if start is None or end is None or end < start:
            raise NewsVetoError(f"bad event window: {event.get('name')!r}")
        symbols = event.get("symbols", "all")
        out.append({"name": str(event.get("name")), "start": start, "end": end,
                    "symbols": "all" if symbols == "all" else {str(s).upper() for s in symbols}})
    return out


class NewsVeto:
    def __init__(self, *, metadata_path: str | Path | None = None, events_path: str | Path | None = None,
                 **config: Any) -> None:
        unknown = set(config) - set(DEFAULTS)
        if unknown:
            raise NewsVetoError(f"unknown config: {sorted(unknown)}")
        self.config = {**DEFAULTS, **config}
        self.metadata_path = Path(metadata_path) if metadata_path else None
        self.events = load_events(events_path) if events_path else []  # a bad calendar refuses to start
        self.events_path = str(events_path) if events_path else None

    def gate(self, symbol: str | None, now: datetime) -> dict[str, Any]:
        """The recorded filter outcome: ``outcome`` is ``pass`` or ``veto``; ``reason`` names why."""
        cfg, sym = self.config, (symbol or "").upper()
        now = now.astimezone(timezone.utc)
        detail: dict[str, Any] = {"symbol": sym, "config": {k: cfg[k] for k in sorted(cfg)},
                                  "metadata": str(self.metadata_path) if self.metadata_path else None,
                                  "events": self.events_path}
        pad = timedelta(minutes=float(cfg["event_pad_minutes"]))
        for event in self.events:
            if event["start"] - pad <= now <= event["end"] + pad and (
                    event["symbols"] == "all" or sym in event["symbols"]):
                return {"name": "news_event", "outcome": "veto",
                        "reason": f"event_blackout:{event['name']}", **detail}
        if self.metadata_path is None:
            return {"name": "news_event", "outcome": "pass", "reason": "no_news_feed_configured", **detail}
        try:
            headlines, heartbeat, refused = load_headlines(self.metadata_path, tail_bytes=int(cfg["tail_bytes"]))
        except OSError as exc:
            detail["feed_error"] = type(exc).__name__
            return self._feed_down(detail)
        detail["refused_lines"] = refused
        if heartbeat is None or (now - heartbeat).total_seconds() > float(cfg["max_heartbeat_age_seconds"]):
            detail["last_heartbeat"] = heartbeat.isoformat() if heartbeat else None
            return self._feed_down(detail)
        window = timedelta(minutes=float(cfg["lookback_minutes"]))
        recent = [h for h in headlines if sym in h["symbols"] and timedelta(0) <= now - h["received"] <= window]
        if recent:
            detail["headline_ids"] = [h["id"] for h in recent][-5:]
            return {"name": "news_event", "outcome": "veto",
                    "reason": f"news_recent:{sym}:{len(recent)}", **detail}
        return {"name": "news_event", "outcome": "pass", "reason": "no_recent_news", **detail}

    def _feed_down(self, detail: dict[str, Any]) -> dict[str, Any]:
        if self.config["required"]:
            return {"name": "news_event", "outcome": "veto", "reason": "news_feed_unavailable", **detail}
        return {"name": "news_event", "outcome": "pass", "reason": "news_feed_unavailable_not_required", **detail}


def append_heartbeat(path: str | Path, at: datetime) -> None:
    _append(path, {"type": "heartbeat", "at": at.astimezone(timezone.utc).isoformat()})


def append_headline(path: str | Path, *, article_id: Any, created_at: str, received_at: str,
                    symbols: list[str], source: str | None) -> None:
    """Metadata only. The caller never passes text; there is no parameter for it."""
    _append(path, {"type": "headline", "id": article_id, "created_at": created_at,
                   "received_at": received_at, "symbols": [str(s).upper() for s in symbols],
                   "source": source})


def _append(path: str | Path, row: Mapping[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o640)
    try:
        os.write(fd, (json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))
    finally:
        os.close(fd)
