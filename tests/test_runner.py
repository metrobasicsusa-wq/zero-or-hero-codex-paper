"""Runner orchestration tests: no credentials, network, scheduler or real state."""
import contextlib
import copy
from datetime import datetime, timedelta
import io
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from zerotrader import runner
from zerotrader.data import NY
from zerotrader.engine import new_state


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.directory = self.root / 'runtime'
        self.directory.mkdir()
        config = json.loads((Path(__file__).resolve().parents[1] / 'settings.json').read_text())
        config['enabled'] = True
        (self.root / 'settings.json').write_text(json.dumps(config))
        self.now = datetime(2026, 10, 1, 11, 0, tzinfo=NY)
        self.snap = {'at': self.now.isoformat(), 'mode': 'PAPER', 'enabled': True,
                     'paused': False, 'halted': None, 'equity': 100000, 'cash': 100000,
                     'drawdown': 0, 'market_open': True, 'last_research': None,
                     'actions': [], 'alerts': [], 'sleeves': {}, 'signals': {}}
        self.broker = Mock()
        self.broker.fill_activities.return_value = []
        self.broker.clock.return_value = {'timestamp': (self.now + timedelta(minutes=2)).isoformat()}
        self.engine = Mock()
        self.engine.alerts = []
        self.engine.audit_available = True
        self.engine.cycle.side_effect = lambda signals, execute: copy.deepcopy(self.snap)
        self.signals = {'swing': [{'symbol': 'AAPL'}], 'intraday': []}
        self.output = io.StringIO()
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(patch.object(runner, 'ROOT', self.root))
        self.stack.enter_context(patch.object(runner, 'data_dir', return_value=self.directory))
        self.stack.enter_context(patch.object(runner, 'process_lock', return_value=contextlib.nullcontext()))
        self.stack.enter_context(patch.object(runner, 'PaperBroker', return_value=self.broker))
        self.stack.enter_context(patch.object(runner, 'Engine', return_value=self.engine))
        self.refresh = self.stack.enter_context(patch.object(runner, 'refresh', return_value=({}, {})))
        self.make_signals = self.stack.enter_context(patch.object(runner, 'make_signals', return_value=self.signals))
        self.publish = self.stack.enter_context(patch.object(runner, 'publish'))
        self.stack.enter_context(contextlib.redirect_stdout(self.output))

    def tearDown(self):
        self.stack.close()
        self.tmp.cleanup()

    def run_action(self, action='cycle'):
        with patch('sys.argv', ['run_trader.py', action]):
            runner.main()

    def test_risk_cycle_runs_before_history_failure_and_no_signal_execution_follows(self):
        events = []
        def cycle(signals, execute):
            events.append(('cycle', signals, execute))
            return copy.deepcopy(self.snap)
        def refresh(*args, **kwargs):
            events.append(('refresh',))
            raise RuntimeError('historical data unavailable')
        self.engine.cycle.side_effect = cycle
        self.refresh.side_effect = refresh
        self.run_action()
        self.assertEqual(events, [('cycle', {}, True), ('refresh',)])
        self.make_signals.assert_not_called()
        saved = json.loads((self.directory / 'latest.json').read_text())
        self.assertTrue(any('Selection/research' in alert for alert in saved['alerts']))

    def test_execution_error_publishes_alert_and_returns_nonzero(self):
        self.engine.cycle.side_effect = [copy.deepcopy(self.snap), RuntimeError('Paper order outcome is uncertain.')]
        with self.assertRaises(SystemExit) as caught:
            self.run_action()
        self.assertEqual(caught.exception.code, 1)
        self.assertEqual(self.engine.cycle.call_count, 2)
        self.publish.assert_called_once()
        saved = json.loads((self.directory / 'latest.json').read_text())
        self.assertTrue(any('Execution stopped' in alert for alert in saved['alerts']))
        self.assertEqual(json.loads(self.output.getvalue())['new_entries'], 'blocked')
        self.assertFalse((self.directory / 'equity-history.json').exists())

    def test_signal_generation_uses_refreshed_clock_after_history_download(self):
        self.run_action()
        used_now = self.make_signals.call_args.args[-1]
        self.assertEqual(used_now, self.now + timedelta(minutes=2))
        self.assertEqual(self.engine.cycle.call_args_list[0].args, ({},))
        self.assertEqual(self.engine.cycle.call_args_list[1].args, (self.signals,))
        self.assertTrue(all(call.kwargs['execute'] for call in self.engine.cycle.call_args_list))

    def test_preview_never_enables_execution(self):
        self.run_action('preview')
        self.assertEqual(self.engine.cycle.call_count, 2)
        self.assertTrue(all(not call.kwargs['execute'] for call in self.engine.cycle.call_args_list))

    def test_first_risk_cycle_failure_prevents_research_and_new_entries(self):
        self.engine.cycle.side_effect = RuntimeError('Account differs from the bound paper account')
        with self.assertRaises(SystemExit) as caught:
            self.run_action()
        self.assertEqual(caught.exception.code, 1)
        self.refresh.assert_not_called()
        self.make_signals.assert_not_called()

    def test_pause_changes_local_state_without_constructing_broker(self):
        self.run_action('pause')
        runner.PaperBroker.assert_not_called()
        self.engine.cycle.assert_not_called()
        self.assertTrue(json.loads((self.directory / 'state.json').read_text())['paused'])

    def test_inputs_and_all_samples_are_archived_before_selection_execution(self):
        observed = []
        def cycle(signals, execute):
            if signals:
                with contextlib.closing(sqlite3.connect(self.directory / 'archive' / 'research.sqlite3')) as db:
                    observed.append(db.execute('SELECT COUNT(*) FROM samples').fetchone()[0])
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM datasets WHERE name IN ('daily','intraday')").fetchone()[0], 2)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM events WHERE kind='selection'").fetchone()[0], 1)
            return copy.deepcopy(self.snap)
        self.engine.cycle.side_effect = cycle
        self.run_action()
        self.assertEqual(observed, [60])
        with contextlib.closing(sqlite3.connect(self.directory / 'archive' / 'research.sqlite3')) as db:
            self.assertEqual(db.execute('SELECT status FROM runs').fetchone()[0], 'completed')

    def test_archive_initialization_failure_preserves_risk_cycle_and_blocks_entries(self):
        with patch.object(runner, 'ResearchArchive', side_effect=OSError('disk unavailable')):
            self.run_action()
        self.assertEqual(self.engine.cycle.call_args_list[0].args, ({},))
        self.assertTrue(self.engine.cycle.call_args_list[0].kwargs['execute'])
        self.assertFalse(self.engine.audit_available)
        self.assertTrue(any('archive unavailable' in alert for alert in self.engine.alerts))

    def test_failed_input_archive_prevents_selection_and_second_execution(self):
        with patch.object(runner.ResearchArchive, 'dataset', side_effect=OSError('disk full')):
            self.run_action()
        self.assertEqual(self.engine.cycle.call_count, 1)
        self.make_signals.assert_not_called()
        with contextlib.closing(sqlite3.connect(self.directory / 'archive' / 'research.sqlite3')) as db:
            self.assertEqual(db.execute('SELECT status FROM runs').fetchone()[0], 'completed_with_alerts')

    def test_uncertain_execution_is_retained_as_failed_run(self):
        self.engine.cycle.side_effect = [copy.deepcopy(self.snap), RuntimeError('uncertain broker result')]
        with self.assertRaises(SystemExit):
            self.run_action()
        with contextlib.closing(sqlite3.connect(self.directory / 'archive' / 'research.sqlite3')) as db:
            status, error = db.execute('SELECT status,error_json FROM runs').fetchone()
        self.assertEqual(status, 'failed')
        self.assertIn('uncertain broker result', error)

    def test_export_is_offline_and_produces_manifest(self):
        self.run_action('export')
        runner.PaperBroker.assert_not_called()
        self.engine.cycle.assert_not_called()
        result = json.loads(self.output.getvalue())
        self.assertTrue((Path(result['directory']) / 'manifest.json').is_file())
        self.assertTrue(Path(result['files']['samples']).is_file())


class SignalFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 10, 1, 11, tzinfo=NY)
        self.broker = Mock()
        self.broker.calendar.return_value = [{'date': '2026-09-30'}, {'date': '2026-10-01'}]
        self.state = new_state()
        runner.seed_strategies(self.state)

    def test_stale_benchmark_aborts_selection(self):
        stale = {'SPY': [{'t': '2026-09-29T04:00:00Z'}]}
        with self.assertRaisesRegex(RuntimeError, 'benchmark is stale'):
            runner.make_signals(self.broker, self.state, stale, {}, self.now)

    def test_missing_held_daily_bar_omits_swing_rebalance(self):
        self.state['owners']['AAPL'] = {'sleeve': 'swing'}
        daily = {'SPY': [{'t': '2026-09-30T04:00:00Z'}],
                 'AAPL': [{'t': '2026-09-29T04:00:00Z'}]}
        with patch.object(runner.swing, 'rank', return_value=[]), patch.object(runner.intraday, 'rank', return_value=[]):
            result = runner.make_signals(self.broker, self.state, daily, {}, self.now)
        self.assertNotIn('swing', result)
        self.assertEqual(result['intraday'], [])


if __name__ == '__main__':
    unittest.main()
