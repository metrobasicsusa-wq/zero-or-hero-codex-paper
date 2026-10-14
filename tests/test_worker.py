import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import trader_worker as worker


class FakeChild:
    pid = 424242
    returncode = 0

    def __init__(self, timeout=False):
        self.timeout = timeout
        self.killed = False

    def communicate(self, timeout):
        if self.timeout and not self.killed:
            raise subprocess.TimeoutExpired('fake-child', timeout)
        return '{"mode":"PAPER","equity":100000,"secret":"DO_NOT_LOG"}', 'DO_NOT_LOG'

    def kill(self):
        self.killed = True
        self.returncode = -9

    def poll(self):
        return self.returncode


class WorkerTests(unittest.TestCase):
    def test_cadence_uses_latest_clock(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            self.assertEqual(worker.interval_seconds(root), 60)
            worker.write_json(root / 'latest.json', {'market_open': False})
            self.assertEqual(worker.interval_seconds(root), 300)
            worker.write_json(root / 'latest.json', {'market_open': True})
            self.assertEqual(worker.interval_seconds(root), 60)
            (root / 'latest.json').write_text('invalid', encoding='utf-8')
            self.assertEqual(worker.interval_seconds(root), 60)

    def test_secret_and_raw_error_not_logged(self):
        result = worker.safe_summary(json.dumps({'mode': 'PAPER', 'equity': 100000,
                                   'secret': 'DO_NOT_LOG', 'error': 'DO_NOT_LOG',
                                   'at': 'DO_NOT_LOG', 'market_open': False}))
        self.assertNotIn('DO_NOT_LOG', json.dumps(result))
        self.assertEqual(result['equity'], 100000)
        self.assertNotIn('DO_NOT_LOG', json.dumps(worker.safe_summary('DO_NOT_LOG')))
        self.assertNotIn('equity', worker.safe_summary('{"equity":NaN}'))

    def test_success_records_only_safe_output(self):
        child = FakeChild()
        with tempfile.TemporaryDirectory() as folder, patch.object(worker.subprocess, 'Popen', return_value=child) as factory:
            state = {'pid': 1, 'consecutive_errors': 2}
            result = worker.run_child(Path(folder), state)
            command = factory.call_args.args[0]
            self.assertEqual(command[0], worker.sys.executable)
            self.assertEqual(command[-1], 'cycle')
            self.assertEqual(result['outcome'], 'ok')
            self.assertEqual(state['consecutive_errors'], 0)
            self.assertIsNone(state['child_pid'])
            self.assertNotIn('DO_NOT_LOG', (Path(folder) / 'worker-last-output.json').read_text())

    def test_timeout_kills_only_owned_child(self):
        child = FakeChild(timeout=True)
        with tempfile.TemporaryDirectory() as folder, patch.object(worker.subprocess, 'Popen', return_value=child):
            # Monotonic deadline expires before the first communicate call.
            with patch.object(worker.time, 'monotonic', side_effect=[0, 241]):
                state = {'pid': 1, 'consecutive_errors': 0}
                result = worker.run_child(Path(folder), state)
            self.assertTrue(child.killed)
            self.assertTrue(result['timed_out'])
            self.assertEqual(state['consecutive_errors'], 1)

    def test_metadata_failure_does_not_orphan_cycle(self):
        child = FakeChild()
        child.returncode = None
        with tempfile.TemporaryDirectory() as folder, patch.object(worker.subprocess, 'Popen', return_value=child):
            with patch.object(worker, '_heartbeat', side_effect=OSError('metadata unavailable')):
                with self.assertRaises(OSError):
                    worker.run_child(Path(folder), {})
        self.assertTrue(child.killed)

    def test_distinct_lock_prevents_duplicate_workers(self):
        with tempfile.TemporaryDirectory() as folder:
            with worker.worker_lock(folder):
                with self.assertRaises(RuntimeError):
                    with worker.worker_lock(folder):
                        self.fail('duplicate lock acquired')
            with worker.worker_lock(folder):
                pass

    def test_preexisting_stop_flag_prevents_any_child(self):
        with tempfile.TemporaryDirectory() as folder:
            (Path(folder) / 'stop-worker.flag').write_text('stop', encoding='utf-8')
            with patch.object(worker, 'run_child') as run:
                worker.run_worker(folder)
            run.assert_not_called()
            state = json.loads((Path(folder) / 'worker.json').read_text())
            self.assertEqual(state['phase'], 'stopped')

    def test_first_cycle_immediate_then_graceful_stop(self):
        with tempfile.TemporaryDirectory() as folder:
            def cycle(directory, state):
                (directory / 'stop-worker.flag').write_text('stop', encoding='utf-8')
            with patch.object(worker, 'run_child', side_effect=cycle) as run, patch.object(worker.time, 'sleep') as sleep:
                worker.run_worker(folder)
            run.assert_called_once()
            sleep.assert_not_called()


if __name__ == '__main__':
    unittest.main()
