"""E1 closure (OI-9): after the session, collect the evidence the runbook's Closure section asks for.

Standalone, stdlib only, meant to be root-installed beside the E1 start script and run once by a timer.
Order: stop runner/health/alert timers -> read the broker (GET only, raw bodies kept) -> if flat with zero
open orders stop the engine, otherwise leave it running (reduce-only) and record NOT_FLAT -> copy the
engine runtime, strategy state and unit journals -> second broker read after the stop -> hash everything
into MANIFEST.json -> write RESULT.md as a machine-generated observation record (never a success claim
beyond what was read). It places no order, never overwrites an existing output directory and never
substitutes a guess for a missing read: an unreadable broker is reported as UNVERIFIED, not flat.
Paper only. Exit codes: 0 closed flat, 2 not flat / unverified (engine left as it was), 1 refused/error.
"""
import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import HTTPRedirectHandler, Request, build_opener

HOST = 'https://paper-api.alpaca.markets'
DEFAULT_PREFIX = 'ust-epoch2'


def unit_names(prefix=DEFAULT_PREFIX):
    """The unit names of one labelled release: (runner timer, health and alert timers, journal units, engine)."""
    return {'runner_timer': f'{prefix}-runner.timer',
            'watch_timers': (f'{prefix}-health-watch.timer', f'{prefix}-alert-dispatch.timer'),
            'journal': (f'{prefix}-engine.service', f'{prefix}-runner.service', f'{prefix}-health-watch.service',
                        f'{prefix}-alert-dispatch.service', f'{prefix}-start.service'),
            'engine': f'{prefix}-engine.service'}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


def read_credentials(path):
    info = Path(path).stat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError('credential_permissions')
    vals = {}
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        t = line.strip()
        if not t or t.startswith('#') or '=' not in t:
            continue
        k, v = t.split('=', 1)
        k = k.strip()
        if k in ('APCA_API_KEY_ID', 'APCA_API_SECRET_KEY'):
            if k in vals:
                raise ValueError('duplicate_credential_field')
            vals[k] = v.strip().strip('"').strip("'")
    key, secret = vals.get('APCA_API_KEY_ID', ''), vals.get('APCA_API_SECRET_KEY', '')
    if not (16 <= len(key) <= 64 and key.isalnum() and len(secret) >= 20):
        raise ValueError('credential_fields_unavailable')
    return key, secret


def make_http_get(key, secret):
    opener = build_opener(_NoRedirect())

    def get(path):
        req = Request(HOST + path, headers={'APCA-API-KEY-ID': key, 'APCA-API-SECRET-KEY': secret}, method='GET')
        with opener.open(req, timeout=20) as resp:
            raw = resp.read(4_000_001)
            if len(raw) > 4_000_000:
                raise ValueError('response_too_large')
            return resp.status, raw
    return get


def broker_read(get, expected_pin_sha256, session_date, out_dir, label):
    """GET-only read. Raw bodies are saved byte for byte. Returns a verdict dict; never raises."""
    verdict = {'label': label, 'started_utc': utc_now(), 'files': {}, 'errors': {}}
    paths = {
        'account': '/v2/account',
        'positions': '/v2/positions',
        'open_orders': '/v2/orders?status=open&limit=500&nested=true',
        'orders_session': f'/v2/orders?status=all&after={session_date}T00:00:00Z&limit=500&direction=asc&nested=true',
        'clock': '/v2/clock',
    }
    bodies = {}
    for name, path in paths.items():
        try:
            status, raw = get(path)
            fn = f'{label}_{name}.json'
            Path(out_dir, fn).write_bytes(raw)
            verdict['files'][fn] = {'http_status': status, 'received_utc': utc_now()}
            bodies[name] = json.loads(raw)
        except Exception as exc:  # noqa: BLE001 - recorded, never hidden
            verdict['errors'][name] = type(exc).__name__
    acct = bodies.get('account')
    pin_ok = False
    if isinstance(acct, dict) and isinstance(acct.get('id'), str):
        pin_ok = hashlib.sha256(acct['id'].encode()).hexdigest() == expected_pin_sha256
    verdict['account_pin_matched'] = pin_ok
    pos, oo = bodies.get('positions'), bodies.get('open_orders')
    verdict['positions_complete'] = isinstance(pos, list)
    verdict['open_orders_complete'] = isinstance(oo, list) and len(oo) < 500
    verdict['position_count'] = len(pos) if isinstance(pos, list) else None
    verdict['open_order_count'] = len(oo) if isinstance(oo, list) else None
    so = bodies.get('orders_session')
    verdict['session_orders_complete'] = isinstance(so, list) and len(so) < 500
    if verdict['session_orders_complete']:
        verdict['session_order_summary'] = [
            {k: o.get(k) for k in ('client_order_id', 'symbol', 'side', 'qty', 'filled_qty', 'filled_avg_price',
                                   'order_class', 'status', 'submitted_at', 'filled_at')}
            for o in so if isinstance(o, dict)]
    clock = bodies.get('clock')
    verdict['clock'] = {k: clock.get(k) for k in ('is_open', 'timestamp', 'next_close')} if isinstance(clock, dict) else None
    # Flat only on positive evidence: pin matched, both lists complete and empty.
    verdict['flat_and_no_open_orders'] = bool(pin_ok and verdict['positions_complete'] and verdict['open_orders_complete']
                                              and not pos and not oo)
    verdict['finished_utc'] = utc_now()
    return verdict


def systemctl(run, *args):
    r = run(['systemctl', *args])
    return {'args': list(args), 'returncode': r.returncode}


def copy_tree(src, dst):
    src, dst = Path(src), Path(dst)
    if not src.exists():
        return {'src': str(src), 'copied': False, 'reason': 'missing'}
    shutil.copytree(src, dst, symlinks=False, dirs_exist_ok=False)
    return {'src': str(src), 'copied': True}


def journal_for(run, unit, out_path, since):
    try:
        r = run(['journalctl', '-u', unit, '--since', since, '--no-pager', '-o', 'short-iso'], capture=True)
    except subprocess.TimeoutExpired:
        # A slow journal must not lose the summary (OI-20): record it and go on.
        Path(out_path).write_text('', encoding='utf-8')
        return 'timeout'
    Path(out_path).write_text(r.stdout or '', encoding='utf-8')
    return r.returncode


def write_manifest(out_dir):
    out_dir = Path(out_dir)
    entries = {}
    for p in sorted(out_dir.rglob('*')):
        if p.is_file() and p.name != 'MANIFEST.json':
            entries[str(p.relative_to(out_dir)).replace(os.sep, '/')] = {'sha256': sha256_file(p), 'bytes': p.stat().st_size}
    manifest = {'created_utc': utc_now(), 'files': entries}
    mp = out_dir / 'MANIFEST.json'
    mp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding='utf-8')
    return sha256_file(mp)


def decision_counts(ledger_path):
    """Counts from the copied decision ledger (COD-62 / OI-27): actions by kind and status, filter vetoes by
    name and reason, and judge abstentions by reason, so a day of refusals by a stopped news writer reads
    differently from a quiet day. Counts only; it judges nothing. None when there is no ledger."""
    path = Path(ledger_path)
    if not path.is_file():
        return None
    actions, vetoes, quality, unreadable = Counter(), Counter(), Counter(), 0
    with path.open(encoding='utf-8', errors='replace') as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except ValueError:
                unreadable += 1
                continue
            if not isinstance(row, dict):
                unreadable += 1
                continue
            action = row.get('action') if isinstance(row.get('action'), dict) else {}
            actions[f"{action.get('kind')}:{action.get('status')}"] += 1
            for f in row.get('filters') or []:
                if isinstance(f, dict) and f.get('outcome') == 'veto':
                    vetoes[f"{f.get('name')}:{f.get('reason')}"] += 1
            gate = row.get('quality_gate') if isinstance(row.get('quality_gate'), dict) else {}
            if gate.get('allow') is False and gate.get('abstain_reason'):
                quality[str(gate['abstain_reason'])] += 1
    return {'records': sum(actions.values()), 'unreadable_lines': unreadable,
            'actions': dict(sorted(actions.items())), 'filter_vetoes': dict(sorted(vetoes.items())),
            'quality_abstentions': dict(sorted(quality.items()))}


def render_result(summary):
    a, b = summary['read_before_stop'], summary.get('read_after_stop')
    lines = [f"# E1 closure record, session {summary['session_date']} (machine generated)", '',
             'Generated by `e1_closure.py`. These are point-in-time observations, not monitoring. Nothing here claims',
             'that protection, exit or recovery were proven: read the order list and the journals for that.', '',
             f"**Verdict: {summary['verdict']}**", '',
             f"- Started {summary['started_utc']}, finished {summary['finished_utc']}.",
             f"- Timer stops (runner first): {json.dumps(summary['timer_stops'])}"]

    def one(label, r):
        if r is None:
            return [f'- {label}: not performed.']
        out = [f"- {label} ({r['started_utc']} to {r['finished_utc']}): pin matched={r['account_pin_matched']}, "
               f"positions={r['position_count']}, open orders={r['open_order_count']}, flat verified={r['flat_and_no_open_orders']}"
               + (f", read errors={r['errors']}" if r['errors'] else '')]
        return out
    lines += one('Broker read before stop', a) + one('Broker read after engine stop', b)
    lines.append(f"- Engine stop: {json.dumps(summary['engine_stop'])}")
    lines.append(f"- Copies: {json.dumps(summary['copies'])}")
    lines += ['', '## Orders the broker lists for the session (summary; raw bodies in the read files)', '']
    sess = (a or {}).get('session_order_summary')
    if sess is None:
        lines.append('Not read completely; no list given.')
    elif not sess:
        lines.append('None.')
    else:
        for o in sess:
            lines.append('- ' + json.dumps(o, sort_keys=True))
    if 'decision_counts' in summary:
        counts = summary['decision_counts']
        lines += ['', '## Runner decisions by reason (copied decision ledger; counts, not judgements)', '']
        if counts is None:
            lines.append('No decision ledger in the strategy state copy.')
        else:
            lines.append(f"- Records: {counts['records']}; unreadable lines: {counts['unreadable_lines']}")
            for key in ('actions', 'filter_vetoes', 'quality_abstentions'):
                lines.append(f"- {key.replace('_', ' ').capitalize()}: {json.dumps(counts[key], sort_keys=True) if counts[key] else 'none'}")
    lines += ['', f"Manifest SHA-256 (MANIFEST.json): `{summary['manifest_sha256']}`", '']
    return '\n'.join(lines)


def close(args, get, run, now_date=None):
    started = utc_now()
    today = now_date or datetime.now(timezone.utc).strftime('%Y-%m-%d')
    names = unit_names(getattr(args, 'unit_prefix', DEFAULT_PREFIX))
    allowed_dates = getattr(args, 'session_dates', None) or [args.session_date]
    if today not in allowed_dates or (getattr(args, 'session_dates', None) is None and today != args.session_date):
        print(f'REFUSED: today {today} is not a session date ({", ".join(allowed_dates)})', file=sys.stderr)
        return 1
    args.session_date = today
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = Path(args.out_root) / f'e1_closure_{args.session_date}_{stamp}'
    if out.exists():
        print('REFUSED: output exists (never overwritten)', file=sys.stderr)
        return 1
    out.mkdir(parents=True, mode=0o700)
    # Only the runner stops before the broker read. The health and alert timers keep watching until the
    # read says flat, so a not-flat result never leaves the engine unwatched (OI-20).
    timer_stops = [systemctl(run, 'stop', names['runner_timer'])]
    read_a = broker_read(get, args.pin_sha256, args.session_date, out, 'before_stop')
    engine_stop = {'attempted': False}
    read_b = None
    if read_a['flat_and_no_open_orders']:
        timer_stops += [systemctl(run, 'stop', t) for t in names['watch_timers']]
        engine_stop = {'attempted': True, **systemctl(run, 'stop', names['engine'])}
    copies = {}
    for name, src in (('engine_runtime', args.runtime_dir), ('strategy_state', args.strategy_dir)):
        copies[name] = copy_tree(src, out / name)
    for unit in names['journal']:
        copies[f'journal_{unit}'] = journal_for(run, unit, out / f'journal_{unit}.txt', f'{args.session_date} 00:00:00 UTC')
    if engine_stop['attempted']:
        read_b = broker_read(get, args.pin_sha256, args.session_date, out, 'after_stop')
    closed = bool(read_a['flat_and_no_open_orders'] and engine_stop.get('returncode') == 0
                  and read_b and read_b['flat_and_no_open_orders'])
    if closed:
        verdict = 'CLOSED_FLAT'
    elif not engine_stop['attempted']:
        verdict = 'NOT_FLAT_OR_UNVERIFIED_ENGINE_LEFT_RUNNING'
    elif engine_stop.get('returncode') != 0:
        verdict = 'ENGINE_STOP_FAILED'
    else:
        verdict = 'STOPPED_BUT_SECOND_READ_NOT_FLAT'
    summary = {'session_date': args.session_date, 'started_utc': started, 'verdict': verdict, 'timer_stops': timer_stops,
               'read_before_stop': read_a, 'read_after_stop': read_b, 'engine_stop': engine_stop, 'copies': copies}
    try:
        summary['decision_counts'] = decision_counts(out / 'strategy_state' / 'decision-ledger.jsonl')
    except OSError as exc:  # a count must never cost the closure its record
        summary['decision_counts_error'] = f'{type(exc).__name__}'
    (out / 'summary.json').write_text(json.dumps(summary, indent=2, sort_keys=True), encoding='utf-8')
    summary['finished_utc'] = utc_now()
    summary['manifest_sha256'] = 'pending'
    (out / 'RESULT.md').write_text(render_result(summary), encoding='utf-8')
    summary['manifest_sha256'] = write_manifest(out)
    (out / 'MANIFEST_SHA256.txt').write_text(summary['manifest_sha256'] + '\n', encoding='utf-8')
    print(f'{verdict} {out} manifest_sha256={summary["manifest_sha256"]}')
    return 0 if closed else 2


def _real_run(cmd, capture=False):
    return subprocess.run(cmd, capture_output=capture, text=True, timeout=60, check=False)


def main(argv=None):
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument('--session-date')
    g.add_argument('--session-dates', help='comma-separated; closes whichever of them is today (labelled releases)')
    p.add_argument('--label', default='epoch2',
                   help='names the units and directories: ust-<label>-*, /var/lib/ust-<label> (default epoch2 = E1)')
    p.add_argument('--out-root')
    p.add_argument('--runtime-dir')
    p.add_argument('--strategy-dir')
    p.add_argument('--env', default='/etc/ust-engine/nvda-paper.env')
    p.add_argument('--migration', default='/etc/ust-engine/nvda-migration.json')
    a = p.parse_args(argv)
    if not re.fullmatch(r'[a-z][a-z0-9]{0,15}', a.label):
        p.error('label must be lowercase letters and digits')
    a.unit_prefix = f'ust-{a.label}'
    a.out_root = a.out_root or f'/var/lib/ust-{a.label}-closure'
    a.runtime_dir = a.runtime_dir or f'/var/lib/ust-{a.label}'
    a.strategy_dir = a.strategy_dir or f'/var/lib/ust-strategy-{a.label}'
    a.session_dates = a.session_dates.split(',') if a.session_dates else None
    if a.session_dates is not None:
        a.session_date = a.session_dates[0]
    a.pin_sha256 = json.loads(Path(a.migration).read_text())['target_account_id_sha256']
    key, secret = read_credentials(a.env)
    return close(a, make_http_get(key, secret), _real_run)


if __name__ == '__main__':
    raise SystemExit(main())
