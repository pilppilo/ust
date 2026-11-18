"""Synthetic tests for e1_closure.py (OI-9). Fake broker and fake systemctl; no network, no VPS."""
import hashlib
import json
import tempfile
import types
import unittest
from pathlib import Path

import e1_closure as ec

ACCOUNT = {'id': 'acct-123', 'status': 'ACTIVE'}
PIN = hashlib.sha256(b'acct-123').hexdigest()
DATE = '2026-11-13'


class FakeBroker:
    def __init__(self, positions=None, open_orders=None, session_orders=None, fail=(), account=None):
        self.positions = [] if positions is None else positions
        self.open_orders = [] if open_orders is None else open_orders
        self.session_orders = [] if session_orders is None else session_orders
        self.fail, self.account, self.calls = set(fail), account or ACCOUNT, []

    def __call__(self, path):
        self.calls.append(path)
        for f in self.fail:
            if f in path:
                raise OSError('down')
        if path.startswith('/v2/account'):
            body = self.account
        elif path.startswith('/v2/positions'):
            body = self.positions
        elif 'status=open' in path:
            body = self.open_orders
        elif 'status=all' in path:
            body = self.session_orders
        else:
            body = {'is_open': False, 'timestamp': 't', 'next_close': 'n'}
        return 200, json.dumps(body).encode()


class FakeRun:
    def __init__(self, stop_engine_rc=0):
        self.cmds, self.stop_engine_rc = [], stop_engine_rc

    def __call__(self, cmd, capture=False):
        self.cmds.append(cmd)
        rc = self.stop_engine_rc if cmd[:3] == ['systemctl', 'stop', 'ust-epoch2-engine.service'] else 0
        return types.SimpleNamespace(returncode=rc, stdout='journal line\n' if cmd[0] == 'journalctl' else '')


class ClosureTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        t = Path(self.tmp.name)
        (t / 'rt').mkdir()
        (t / 'rt' / 'journal.jsonl').write_text('{"e":1}\n')
        (t / 'st').mkdir()
        (t / 'st' / 'cycles.jsonl').write_text('{"c":1}\n')
        self.args = types.SimpleNamespace(session_date=DATE, out_root=str(t / 'out'), runtime_dir=str(t / 'rt'),
                                          strategy_dir=str(t / 'st'), pin_sha256=PIN)
        Path(self.args.out_root).mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def run_close(self, broker, run=None):
        run = run or FakeRun()
        rc = ec.close(self.args, broker, run, now_date=DATE)
        outs = list(Path(self.args.out_root).iterdir())
        return rc, run, (outs[0] if outs else None)

    def test_flat_closes_and_hashes_everything(self):
        rc, run, out = self.run_close(FakeBroker())
        self.assertEqual(rc, 0)
        man = json.loads((out / 'MANIFEST.json').read_text())
        for rel, meta in man['files'].items():
            self.assertEqual(ec.sha256_file(out / rel), meta['sha256'])
        for needed in ('before_stop_positions.json', 'after_stop_open_orders.json', 'engine_runtime/journal.jsonl',
                       'strategy_state/cycles.jsonl', 'RESULT.md', 'summary.json'):
            self.assertIn(needed, man['files'])
        self.assertIn('CLOSED_FLAT', (out / 'RESULT.md').read_text())
        self.assertEqual((out / 'MANIFEST_SHA256.txt').read_text().strip(), ec.sha256_file(out / 'MANIFEST.json'))

    def test_runner_timer_stopped_first_then_engine_after_flat_read(self):
        _, run, _ = self.run_close(FakeBroker())
        stops = [c[2] for c in run.cmds if c[:2] == ['systemctl', 'stop']]
        self.assertEqual(stops[0], 'ust-epoch2-runner.timer')
        self.assertEqual(stops[-1], 'ust-epoch2-engine.service')

    def test_open_position_leaves_engine_running(self):
        rc, run, out = self.run_close(FakeBroker(positions=[{'symbol': 'SPY', 'qty': '1'}]))
        self.assertEqual(rc, 2)
        self.assertNotIn(['systemctl', 'stop', 'ust-epoch2-engine.service'], run.cmds)
        self.assertIn('NOT_FLAT', (out / 'RESULT.md').read_text())

    def test_open_order_leaves_engine_running(self):
        rc, run, _ = self.run_close(FakeBroker(open_orders=[{'id': 'x'}]))
        self.assertEqual(rc, 2)
        self.assertNotIn(['systemctl', 'stop', 'ust-epoch2-engine.service'], run.cmds)

    def test_unreadable_positions_is_not_flat(self):
        rc, run, out = self.run_close(FakeBroker(fail=('/v2/positions',)))
        self.assertEqual(rc, 2)
        self.assertNotIn(['systemctl', 'stop', 'ust-epoch2-engine.service'], run.cmds)
        self.assertIn('positions', json.loads((out / 'summary.json').read_text())['read_before_stop']['errors'])

    def test_wrong_account_pin_is_not_flat(self):
        rc, run, _ = self.run_close(FakeBroker(account={'id': 'other'}))
        self.assertEqual(rc, 2)
        self.assertNotIn(['systemctl', 'stop', 'ust-epoch2-engine.service'], run.cmds)

    def test_full_open_orders_page_is_incomplete_not_flat(self):
        rc, _, _ = self.run_close(FakeBroker(open_orders=[{'id': str(i)} for i in range(500)]))
        self.assertEqual(rc, 2)

    def test_engine_stop_failure_is_not_closed(self):
        rc, _, out = self.run_close(FakeBroker(), FakeRun(stop_engine_rc=1))
        self.assertEqual(rc, 2)
        self.assertNotIn('CLOSED_FLAT', (out / 'RESULT.md').read_text().split('Verdict:')[1].split('\n')[0])

    def test_second_read_not_flat_after_stop_is_reported(self):
        class Flaky(FakeBroker):
            n = 0

            def __call__(self, path):
                if path.startswith('/v2/positions'):
                    Flaky.n += 1
                    self.positions = [] if Flaky.n == 1 else [{'symbol': 'SPY', 'qty': '1'}]
                return super().__call__(path)
        rc, _, out = self.run_close(Flaky())
        self.assertEqual(rc, 2)
        self.assertIn('STOPPED_BUT_SECOND_READ_NOT_FLAT', (out / 'RESULT.md').read_text())

    def test_session_orders_recorded_raw_and_summarised(self):
        orders = [{'client_order_id': 'c1', 'symbol': 'SPY', 'side': 'buy', 'status': 'filled', 'filled_avg_price': '500.1',
                   'legs': [{'status': 'canceled'}]}]
        _, _, out = self.run_close(FakeBroker(session_orders=orders))
        raw = json.loads((out / 'before_stop_orders_session.json').read_text())
        self.assertEqual(raw[0]['legs'][0]['status'], 'canceled')
        self.assertIn('"client_order_id": "c1"', (out / 'RESULT.md').read_text())

    def test_wrong_date_refuses_before_anything(self):
        run = FakeRun()
        rc = ec.close(self.args, FakeBroker(), run, now_date='2026-11-14')
        self.assertEqual(rc, 1)
        self.assertEqual(run.cmds, [])
        self.assertEqual(list(Path(self.args.out_root).iterdir()), [])

    def test_missing_state_dir_is_recorded_not_fatal(self):
        self.args.strategy_dir = str(Path(self.tmp.name) / 'nope')
        rc, _, out = self.run_close(FakeBroker())
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads((out / 'summary.json').read_text())['copies']['strategy_state']['copied'], False)

    def test_decisions_are_counted_by_reason_so_a_news_outage_is_visible(self):
        # COD-62 / OI-27: a stopped news writer under a required veto shows as vetoes by reason.
        veto = {'name': 'news_event', 'outcome': 'veto', 'reason': 'news_feed_unavailable'}
        rows = [{'action': {'kind': 'abstain', 'status': 'abstained'}, 'filters': [veto]},
                {'action': {'kind': 'abstain', 'status': 'abstained'}, 'filters': [veto]},
                {'action': {'kind': 'entry_order', 'status': 'submitted'},
                 'filters': [{'name': 'news_event', 'outcome': 'pass', 'reason': 'no_recent_news'}]},
                {'action': {'kind': 'abstain', 'status': 'entry_abstained_input_quality'},
                 'quality_gate': {'allow': False, 'abstain_reason': 'quote:spread_wide'}}]
        ledger = Path(self.args.strategy_dir) / 'decision-ledger.jsonl'
        ledger.write_text(''.join(json.dumps(r) + '\n' for r in rows) + 'not json\n')
        rc, _, out = self.run_close(FakeBroker())
        self.assertEqual(rc, 0)
        counts = json.loads((out / 'summary.json').read_text())['decision_counts']
        self.assertEqual(counts['records'], 4)
        self.assertEqual(counts['unreadable_lines'], 1)
        self.assertEqual(counts['filter_vetoes'], {'news_event:news_feed_unavailable': 2})
        self.assertEqual(counts['quality_abstentions'], {'quote:spread_wide': 1})
        self.assertEqual(counts['actions']['entry_order:submitted'], 1)
        self.assertIn('news_event:news_feed_unavailable', (out / 'RESULT.md').read_text())

    def test_no_decision_ledger_is_said_not_counted_as_zero(self):
        rc, _, out = self.run_close(FakeBroker())
        self.assertEqual(rc, 0)
        self.assertIsNone(json.loads((out / 'summary.json').read_text())['decision_counts'])
        self.assertIn('No decision ledger', (out / 'RESULT.md').read_text())

    def test_not_flat_leaves_health_and_alert_timers_running(self):
        # OI-20: a not-flat result must not leave the engine unwatched.
        _, run, _ = self.run_close(FakeBroker(positions=[{'symbol': 'SPY', 'qty': '1'}]))
        stops = [c[2] for c in run.cmds if c[:2] == ['systemctl', 'stop']]
        self.assertEqual(stops, ['ust-epoch2-runner.timer'])

    def test_flat_stops_watchers_after_the_broker_read_and_before_the_engine(self):
        _, run, _ = self.run_close(FakeBroker())
        stops = [c[2] for c in run.cmds if c[:2] == ['systemctl', 'stop']]
        self.assertEqual(stops, ['ust-epoch2-runner.timer', 'ust-epoch2-health-watch.timer',
                                 'ust-epoch2-alert-dispatch.timer', 'ust-epoch2-engine.service'])

    def test_a_labelled_release_uses_its_own_unit_names_only(self):
        self.args.unit_prefix = 'ust-e2'
        _, run, out = self.run_close(FakeBroker())
        touched = {c[2] for c in run.cmds if c[:2] == ['systemctl', 'stop']} | {c[2] for c in run.cmds if c[0] == 'journalctl'}
        self.assertTrue(touched and all(n.startswith('ust-e2-') for n in touched), touched)
        self.assertIn('ust-e2-engine.service', touched)

    def test_a_date_list_closes_today_and_refuses_other_days(self):
        self.args.session_dates = ['2026-11-12', DATE, '2026-11-17']
        rc, _, out = self.run_close(FakeBroker())
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads((out / 'summary.json').read_text())['session_date'], DATE)
        run = FakeRun()
        self.args.session_dates = ['2026-11-12', '2026-11-17']
        self.assertEqual(ec.close(self.args, FakeBroker(), run, now_date=DATE), 1)
        self.assertEqual(run.cmds, [])

    def test_a_journal_timeout_does_not_lose_the_summary(self):
        import subprocess

        class SlowJournal(FakeRun):
            def __call__(self, cmd, capture=False):
                if cmd[0] == 'journalctl':
                    raise subprocess.TimeoutExpired(cmd, 60)
                return super().__call__(cmd, capture)
        rc, _, out = self.run_close(FakeBroker(), SlowJournal())
        self.assertEqual(rc, 0)
        self.assertTrue((out / 'RESULT.md').exists())
        self.assertIn('timeout', json.dumps(json.loads((out / 'summary.json').read_text())['copies']))


    def test_only_get_paths_used(self):
        b = FakeBroker()
        self.run_close(b)
        self.assertTrue(b.calls and all(c.startswith('/v2/') for c in b.calls))


if __name__ == '__main__':
    unittest.main()
