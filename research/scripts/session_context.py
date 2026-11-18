"""Build prior-session IEX context for the first strategy set, data only."""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from desk_broker import AlpacaDeskAdapter, BrokerError, DATA_HOST
from desk_config import PAPER_HOST
from desk_orb import history_window, median_ratios, prior_session_ratios
from input_quality import CALENDAR_YEARS, EARLY_CLOSES, HOLIDAYS

ET = ZoneInfo("America/New_York")
SYMBOLS = ("SPY", "QQQ")


class RecordingDataAdapter(AlpacaDeskAdapter):
    """Retain every successful data response before returning it to the parser."""

    def __init__(self, evidence_dir: Path):
        super().__init__("https://" + PAPER_HOST, allowed_symbols=SYMBOLS)
        self.evidence_dir = evidence_dir
        self.request_index = 0

    def _request(self, path, *, host=PAPER_HOST, method="GET", payload=None):
        if host != DATA_HOST or method != "GET" or payload is not None:
            raise ValueError("context adapter permits data GET requests only")
        response = super()._request(path, host=host, method=method, payload=payload)
        self.request_index += 1
        target = self.evidence_dir / f"response-{self.request_index:04d}.json"
        with target.open("x", encoding="utf-8") as stream:
            json.dump({"path": path, "received_at": datetime.now(timezone.utc).isoformat(),
                       "response": response}, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        return response


def previous_session(day: date) -> date:
    prior = day - timedelta(days=1)
    while True:
        if prior.year not in CALENDAR_YEARS:
            raise ValueError("calendar year unavailable")
        if prior.weekday() < 5 and prior not in HOLIDAYS:
            return prior
        prior -= timedelta(days=1)


def build_context(broker, session_date: date, generated_at: datetime) -> dict:
    if generated_at.tzinfo is None or generated_at.astimezone(ET).date() != session_date:
        raise ValueError("context generation must occur on its session date")
    previous = previous_session(session_date)
    prior_close = EARLY_CLOSES.get(previous, time(16))
    close_at = datetime.combine(previous, prior_close, tzinfo=ET)
    final_minute = close_at - timedelta(minutes=1)
    start, end = history_window(session_date)
    results = {}
    for symbol in SYMBOLS:
        try:
            bars = broker.minute_bars([symbol], start, end).get(symbol, [])
        except BrokerError as exc:
            results[symbol] = {"prior_close": None, "prior_close_time": None,
                               "orb_width_median": None, "orb_history_sessions": 0,
                               "error": type(exc).__name__}
            continue
        final = [bar for bar in bars if bar.start == final_minute]
        ratios = prior_session_ratios({symbol: bars}, session_date.isoformat(), symbols=(symbol,))
        medians = median_ratios(ratios, symbols=(symbol,))
        results[symbol] = {"prior_close": final[0].close if len(final) == 1 else None,
                           "prior_close_time": final[0].start.isoformat() if len(final) == 1 else None,
                           "orb_width_median": medians.get(symbol),
                           "orb_history_sessions": len(ratios.get(symbol, []))}
    return {"version": 1, "feed": "iex", "session_date": session_date.isoformat(),
            "previous_session": previous.isoformat(), "generated_at": generated_at.astimezone(timezone.utc).isoformat(),
            "symbols": results}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("/var/lib/ust-marketdata"))
    parser.add_argument("--evidence-root", type=Path, default=Path("/var/lib/ust-marketdata-private/strategy-context"))
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    session_date = now.astimezone(ET).date()
    stamp = now.strftime("%Y%m%dT%H%M%S%fZ")
    evidence_dir = args.evidence_root / session_date.isoformat() / stamp
    evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
    broker = RecordingDataAdapter(evidence_dir)
    result = build_context(broker, session_date, now)
    completed = datetime.now(timezone.utc)
    if completed.astimezone(ET).date() != session_date:
        raise ValueError("context generation crossed the session date")
    result["generated_at"] = completed.isoformat()
    path = args.output_dir / f"strategy-context-{session_date.isoformat()}.json"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    archived = evidence_dir / "context.json"
    with archived.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(archived.read_bytes())
    os.replace(temporary, path)
    return 1 if any("error" in value for value in result["symbols"].values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
