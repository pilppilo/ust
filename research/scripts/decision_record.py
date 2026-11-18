"""Decision record (build item C2): one append-only, hash-chained record per tick.

A record says what the bot saw (the input quality verdicts and the gate that
followed from them), what each candidate proposed, which filters vetoed, and what
it did or declined to do and why, so that every action and every abstention can be
traced after the fact. It is derived from the runner's own cycle record and adds
nothing the cycle did not observe; it places nothing and reads no broker.

Chain: each line carries ``seq``, ``prev_hash`` and ``hash`` (sha256 of the line's
canonical JSON without ``hash``). ``DecisionLedger`` refuses to open a ledger whose
chain is broken or whose last line is torn, so a corrupted record can never be
silently appended to. Never rewrite or truncate an existing ledger.

Invariants (``validate``): an order action needs ``quality.gate.allow`` true when the
tick was judged; an abstention recorded as input-quality carries its reason; exits are
never blocked by the judge. A tick that was not judged says so (``judged: false``)
rather than implying clean inputs.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping

SCHEMA = "decision-record-v1"
GENESIS = "0" * 64

_ORDER_ENTRY = {"submitted"}
_ORDER_EXIT = {"closed"}
_REFUSED_ENTRY = {"reserve_refused", "prepare_refused", "submit_refused"}
_REFUSED_EXIT = {"close_refused"}
_HOLD = {"holding", "lifecycle_pending"}
_ABSTAIN = {"abstained", "entry_abstained", "entry_abstained_input_quality",
            "entry_abstained_strategy_not_eligible", "entry_abstained_news_veto", "entry_window_closed",
            "session_closed", "paused", "entry_suspended", "inputs_unavailable", "not_owned",
            "entry_symbol_unavailable", "state_unavailable"}


class LedgerError(Exception):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
                      allow_nan=False).encode("ascii")


def record_hash(record: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical({k: v for k, v in record.items() if k != "hash"})).hexdigest()


def action_kind(status: str) -> str:
    if status in _ORDER_ENTRY:
        return "entry_order"
    if status in _ORDER_EXIT:
        return "exit_order"
    if status in _REFUSED_ENTRY:
        return "entry_refused"
    if status in _REFUSED_EXIT:
        return "exit_refused"
    if status in _HOLD:
        return "hold"
    if status in _ABSTAIN:
        return "abstain"
    return "other"


def build_core(cycle: Mapping[str, Any], *, filters: list[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    """The record body (without seq/prev_hash/hash) from one runner cycle record."""
    status = str(cycle.get("status"))
    dispatch = cycle.get("dispatch") if isinstance(cycle.get("dispatch"), Mapping) else None
    quality = cycle.get("quality")
    judged = isinstance(quality, Mapping)
    verdicts = {}
    context: dict[str, Any] = {}
    if judged:
        for name, v in (quality.get("verdicts") or {}).items():
            verdicts[name] = {"verdict": v.get("verdict"), "reasons": list(v.get("reasons") or [])}
        details = ((quality.get("verdicts") or {}).get("quote") or {}).get("details") or {}
        for key in ("bid", "ask", "mid", "spread_bps", "age_seconds", "source"):
            if key in details:
                context[key] = details[key]
        context["config_digest"] = quality.get("config_digest")
    candidates = []
    if dispatch is not None:
        candidates.append({"strategy_revision": dispatch.get("strategy_revision"),
                           "decision_id": dispatch.get("decision_id"),
                           "inputs_sha256": dispatch.get("inputs_sha256"),
                           "outcome": dispatch.get("status"),
                           "reasons": list(dispatch.get("reasons") or [])})
    news = cycle.get("news_gate")
    if isinstance(news, Mapping):
        filters = list(filters or []) + [{"name": news.get("name", "news_event"),
                                          "outcome": "veto" if news.get("outcome") == "veto" else "pass",
                                          "reason": news.get("reason")}]
    reasons = list(cycle.get("reasons") or [])
    if not reasons and dispatch is not None:
        reasons = list(dispatch.get("reasons") or [])
    core = {
        "schema": SCHEMA,
        "run_id": cycle.get("run_id"), "cycle": cycle.get("cycle"),
        "tick_utc": cycle.get("observed_utc"),
        "judged": judged,
        "input_verdicts": verdicts,
        "quality_gate": cycle.get("quality_gate"),
        "strategy_gate": cycle.get("strategy_gate"),
        "context": context,
        "filters": [dict(f) for f in (filters or [])],
        "candidates": candidates,
        "action": {"status": status, "kind": action_kind(status), "reasons": reasons,
                   "entry_symbol": cycle.get("entry_symbol"),
                   "lifecycle_id": cycle.get("lifecycle_id")},
    }
    if "strategy_candidates" in cycle:
        core["strategy_candidates"] = list(cycle["strategy_candidates"])
        core["selected_candidate"] = cycle.get("selected_candidate")
    if "decision_inputs" in cycle:
        core["decision_inputs"] = cycle["decision_inputs"]
    if cycle.get("incident"):
        core["incident"] = cycle["incident"]
    return core


def validate(core: Mapping[str, Any]) -> None:
    """Raise LedgerError if the record contradicts its own invariants."""
    action = core.get("action") or {}
    kind, gate = action.get("kind"), core.get("quality_gate")
    if core.get("judged") and kind == "entry_order" and not (isinstance(gate, Mapping) and gate.get("allow") is True):
        raise LedgerError("entry order recorded without an allowing input-quality gate")
    if action.get("status") == "entry_abstained_input_quality" and not action.get("reasons"):
        raise LedgerError("input-quality abstention without a reason")
    sgate = core.get("strategy_gate")
    if kind == "entry_order" and isinstance(sgate, Mapping) and sgate.get("allow") is not True:
        raise LedgerError("entry order recorded without an allowing strategy gate")
    if action.get("status") == "entry_abstained_strategy_not_eligible" and not action.get("reasons"):
        raise LedgerError("strategy-gate abstention without a reason")
    if action.get("status") == "entry_abstained_news_veto" and not any(
            f.get("outcome") == "veto" for f in core.get("filters") or []):
        raise LedgerError("news-veto abstention without a vetoing filter")
    if kind == "entry_order" and any(f.get("outcome") == "veto" for f in core.get("filters") or []):
        raise LedgerError("entry order recorded despite a vetoing filter")
    for f in core.get("filters") or []:
        if f.get("outcome") not in ("pass", "veto"):
            raise LedgerError("filter outcome must be pass or veto")
    if kind in ("entry_order", "exit_order") and not core.get("tick_utc"):
        raise LedgerError("order action without a tick time")


def verify(path: str | Path) -> tuple[int, str]:
    """Check the whole chain. Returns (records, last_hash); raises LedgerError."""
    path = Path(path)
    if not path.exists():
        return 0, GENESIS
    data = path.read_bytes()
    if data and not data.endswith(b"\n"):
        raise LedgerError("torn last line")
    prev, count = GENESIS, 0
    for number, line in enumerate(data.splitlines(), 1):
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise LedgerError(f"line {number}: not JSON") from exc
        if record.get("seq") != count or record.get("prev_hash") != prev:
            raise LedgerError(f"line {number}: chain broken")
        if record.get("hash") != record_hash(record):
            raise LedgerError(f"line {number}: hash mismatch")
        prev, count = record["hash"], count + 1
    return count, prev


class DecisionLedger:
    """Append-only hash-chained decision log. Opening verifies the existing chain."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.count, self.last_hash = verify(self.path)

    def append(self, core: Mapping[str, Any]) -> dict[str, Any]:
        validate(core)
        record = dict(core)
        record["seq"], record["prev_hash"] = self.count, self.last_hash
        record["hash"] = record_hash(record)
        line = _canonical(record) + b"\n"
        fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o640)
        try:
            remaining = memoryview(line)
            while remaining:
                written = os.write(fd, remaining)
                if written <= 0:
                    raise OSError("decision ledger write made no progress")
                remaining = remaining[written:]
            os.fsync(fd)
        finally:
            os.close(fd)
        self.count, self.last_hash = self.count + 1, record["hash"]
        return record


def explain(record: Mapping[str, Any]) -> list[str]:
    """Plain lines for the cockpit "Why" panel: what was seen, what was decided, why."""
    action = record.get("action") or {}
    lines = [f"{record.get('tick_utc')}: {action.get('kind')} ({action.get('status')})"]
    if not record.get("judged"):
        lines.append("inputs were not judged on this tick")
    for name, v in (record.get("input_verdicts") or {}).items():
        why = ", ".join(v.get("reasons") or []) or "ok"
        lines.append(f"{name}: {v.get('verdict')} ({why})")
    gate = record.get("quality_gate")
    if isinstance(gate, Mapping):
        lines.append("entry gate: " + ("allowed" if gate.get("allow") else f"blocked, {gate.get('abstain_reason')}"))
    for f in record.get("filters") or []:
        lines.append(f"filter {f.get('name')}: {f.get('outcome')} {f.get('reason') or ''}".rstrip())
    for c in record.get("candidates") or []:
        why = ", ".join(c.get("reasons") or [])
        lines.append(f"candidate {c.get('strategy_revision')}: {c.get('outcome')}" + (f" ({why})" if why else ""))
    if action.get("reasons"):
        lines.append("reason: " + ", ".join(str(r) for r in action["reasons"]))
    return lines
