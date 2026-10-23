"""Coordinator fault injection with synthetic state, stores and fake brokers."""
from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from contextlib import closing, nullcontext, redirect_stdout, redirect_stderr

import test_engine
from zerotrader import runner
from zerotrader.archive import ResearchArchive
from zerotrader.archival import capture_fills
from zerotrader.cloud_auxiliary import capture_auxiliary
from zerotrader.cloud_bundle import BundleCorrupt, BundleReceipt, BundleUncertain, RecoveredBundle
from zerotrader.cloud_checkpoint import CheckpointConflict, CheckpointReceipt, CheckpointUncertain, LoadedCheckpoint, _canonical, _transition
from zerotrader.cloud_engine import CloudDurabilityError, CloudEngine
from zerotrader.cloud_runner import (
    CODE_REPOSITORY, STATE_REPOSITORY, CloudCoordinator, CloudRunError,
    account_digest, environment_paper_credentials, main, run_duration, validate_cutover, validate_environment, within_session_window,
)
from zerotrader.data import NY
from zerotrader.engine import new_state
from zerotrader.storage import write_json


class FakeCore:
    def __init__(self, state):
        self.state = deepcopy(state)
        self.receipt = CheckpointReceipt("1" * 40, None, hashlib.sha256(_canonical(state)).hexdigest())
        self.commits, self.fail, self.before = [], False, None
        self.confirm_failure = None

    def confirm(self, receipt):
        if self.confirm_failure:
            raise self.confirm_failure
        if receipt.commit != self.receipt.commit:
            raise CheckpointConflict("SYNTHETIC changed head")
        return LoadedCheckpoint(self.receipt, deepcopy(self.state))

    def commit(self, state, *, expected_head):
        if self.before:
            self.before(state)
        if self.fail:
            raise CheckpointUncertain(self.receipt)
        if expected_head != self.receipt.commit:
            raise RuntimeError("SYNTHETIC mismatched head")
        _transition(self.state, state)
        receipt = CheckpointReceipt(format(len(self.commits) + 2, "040x"), expected_head,
                                    hashlib.sha256(_canonical(state)).hexdigest())
        self.state, self.receipt = deepcopy(state), receipt
        self.commits.append(deepcopy(state))
        return receipt


class FakeBundle:
    def __init__(self, core):
        self.core, self.calls, self.fail = core, [], False
        self.failure = None
        self.archive_path = None

    def commit(self, state, archive_source, *, expected_head):
        if self.fail:
            raise RuntimeError("SYNTHETIC unknown remote failure")
        if self.failure:
            raise self.failure
        receipt = self.core.commit(state, expected_head=expected_head)
        self.archive_path = Path(archive_source)
        self.calls.append((expected_head, deepcopy(state)))
        return BundleReceipt(receipt.commit, receipt.previous_commit, receipt.state_sha256,
                             "a" * 64, "b" * 64, "c" * 64, len(self.calls) - 1)

    def restore_recovery(self, target):
        with closing(sqlite3.connect(self.archive_path)) as source, closing(sqlite3.connect(target)) as destination:
            source.backup(destination)
        return RecoveredBundle(self.core.receipt, deepcopy(self.core.state), {}, self.core.receipt.state_sha256,
                               "a" * 64, "b" * 64, "c" * 64, 0)


class CloudRunnerTests(unittest.TestCase):
    def test_persistence_timing_waits_for_next_natural_cycle(self):
        self.cycle()
        metrics = deepcopy(self.coordinator.pending_cycle_metrics)
        self.assertTrue(metrics['archive_complete'])
        self.assertGreaterEqual(metrics['cycle_including_persistence_seconds'], metrics['final_bundle_seconds'])
        self.assertGreaterEqual(metrics['bundle_persistence_seconds'], metrics['final_bundle_seconds'] * 0.99)
        with self.archive._connect() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM events WHERE kind='cloud_previous_cycle_timing'").fetchone()[0], 0)
        self.cycle()
        with self.archive._connect() as db:
            row = db.execute("SELECT payload_hash FROM events WHERE kind='cloud_previous_cycle_timing'").fetchone()
            self.assertEqual(self.archive._get(db, row[0]), metrics)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM runs WHERE action='cloud_cycle'").fetchone()[0], 2)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="zoh-synthetic-coordinator-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        app = self.root / "app"
        (app / "zerotrader").mkdir(parents=True)
        (app / "zerotrader" / "synthetic.py").write_text("SYNTHETIC = True\n")
        source = patch("zerotrader.archive.ROOT", app)
        source.start()
        self.addCleanup(source.stop)
        self.directory = self.root / "runtime"
        self.archive = ResearchArchive(self.directory)
        self.run_id = self.archive.start_run("synthetic_migration", {}, {})
        self.broker = test_engine.FakeBroker()
        self.broker.open = False
        self.broker.now = datetime(2026, 10, 14, 16, 30, tzinfo=NY)
        self.state = new_state()
        self.state["account_id"] = self.broker.account_value["id"]
        runner.seed_strategies(self.state)
        self.expected = account_digest(self.state)
        self.proof = {"version": 1, "authority": "github-actions", "code_repository": CODE_REPOSITORY,
                      "state_repository": STATE_REPOSITORY, "branch": "main", "account_sha256": self.expected,
                      "local_worker_stopped": True, "local_logon_task_disabled": True,
                      "stopped_at": "2020-01-01T00:00:00Z", "source_commit": "e" * 40, "evidence_cutoff": 1}
        write_json(self.directory / "cloud-cutover.json", self.proof)
        write_json(self.directory / "archive-backfill.json", {"synthetic": True})
        self.core = FakeCore(self.state)
        self.bundle = FakeBundle(self.core)
        self.coordinator = CloudCoordinator(self.core, self.bundle, self.directory, self.state,
                                            self.core.receipt, self.expected)
        self.config = json.loads((Path(__file__).resolve().parents[1] / "settings.json").read_text())
        self.config["enabled"] = True
        self.ops = SimpleNamespace(
            bootstrap=lambda *args: None, capture_fills=lambda *args: 0,
            weekly_research_due=lambda *args: False,
            apply_pending_research=runner.apply_pending_research,
            refresh=lambda *args, **kwargs: ({}, {}),
            make_signals=lambda *args: {}, build_samples=lambda *args: [],
            learn=runner.learn,
        )

    def cycle(self):
        return self.coordinator.run_cycle(self.broker, self.config, operations=self.ops)

    def test_closed_cycle_finalizes_and_bundles_archive_auxiliary_with_shared_head(self):
        result = self.cycle()
        self.assertEqual(result["status"], "completed")
        self.assertEqual(self.broker.submitted, [])
        self.assertGreaterEqual(len(self.bundle.calls), 2)
        self.assertEqual(self.coordinator.receipt.commit, self.core.receipt.commit)
        self.assertTrue(self.coordinator.evidence_ready)
        with self.archive._connect() as db:
            self.assertEqual(db.execute("SELECT status FROM runs WHERE action='cloud_cycle'").fetchone()[0], "completed")
            self.assertGreater(db.execute("SELECT count(*) FROM events WHERE kind='cloud_auxiliary_manifest_v1'").fetchone()[0], 0)

    def test_core_unknown_poison_stops_without_retry_or_submit(self):
        self.core.fail = True
        with self.assertRaises(CloudRunError):
            self.cycle()
        self.assertTrue(self.coordinator.failed)
        self.assertEqual(self.bundle.calls, [])
        self.assertEqual(self.broker.submitted, [])
        self.core.fail = False
        with self.assertRaises(CloudDurabilityError):
            self.cycle()
        self.assertEqual(self.core.commits, [])

    def test_bundle_unknown_poison_does_not_fall_back_to_core_or_retry(self):
        self.bundle.fail = True
        with self.assertRaises(CloudRunError):
            self.cycle()
        count = len(self.core.commits)
        self.assertTrue(self.coordinator.failed)
        self.assertEqual(self.broker.submitted, [])
        with self.assertRaises(CloudDurabilityError):
            self.coordinator.checkpoint(self.state)
        self.assertEqual(len(self.core.commits), count)

    def test_archive_only_error_confirmed_core_remains_usable_for_later_risk_cycles(self):
        self.bundle.failure = BundleCorrupt("SYNTHETIC local export failure")
        result = self.cycle()
        self.assertFalse(self.coordinator.failed)
        self.assertFalse(result["archive_complete"])
        count = len(self.core.commits)
        self.cycle()
        self.assertGreater(len(self.core.commits), count)
        self.assertEqual(self.broker.submitted, [])

    def test_archive_failure_with_changed_core_head_is_poisoned(self):
        self.bundle.failure = BundleCorrupt("SYNTHETIC local export failure")
        self.core.confirm_failure = CheckpointConflict("SYNTHETIC competing writer")
        with self.assertRaises(CloudRunError):
            self.cycle()
        self.assertTrue(self.coordinator.failed)
        self.assertEqual(self.broker.submitted, [])

    def test_post_push_bundle_uncertain_is_never_treated_as_archive_only(self):
        self.bundle.failure = BundleUncertain(self.core.receipt)
        with patch.object(self.core, "confirm", wraps=self.core.confirm) as confirm:
            with self.assertRaises(CloudRunError):
                self.cycle()
        self.assertTrue(self.coordinator.failed)
        confirm.assert_not_called()

    def test_archive_initialization_failure_still_runs_core_risk_checks(self):
        with patch("zerotrader.cloud_runner.ResearchArchive", side_effect=OSError("SYNTHETIC archive outage")):
            result = self.cycle()
        self.assertFalse(result["archive_complete"])
        self.assertTrue(self.core.commits)
        self.assertFalse(self.coordinator.failed)

    def test_archive_initialization_failure_keeps_durable_owned_exit_available(self):
        self.broker.open = True
        self.broker.now = datetime(2026, 10, 14, 15, 50, tzinfo=NY)
        test_engine.EngineTests.owned(self, sleeve="intraday")
        self.core.state = deepcopy(self.state)
        with patch("zerotrader.cloud_runner.ResearchArchive", side_effect=OSError("SYNTHETIC archive outage")):
            result = self.cycle()
        self.assertFalse(result["archive_complete"])
        self.assertEqual([row["side"] for row in self.broker.submitted], ["sell"])
        self.assertTrue(self.broker.canceled)
        self.assertFalse(self.coordinator.failed)

    def test_none_returning_candidate_discard_is_remotely_persisted(self):
        self.state["pending_research"] = {"evaluated_at": "2026-09-01T13:30:00Z",
            "incumbent": deepcopy(self.state["strategies"]), "results": {}}
        self.core.state = deepcopy(self.state)
        self.cycle()
        self.assertNotIn("pending_research", self.core.state)
        self.assertTrue(any(row.get("event") == "research_candidate_discarded" for row in self.core.state["events"]))

    def test_approved_candidate_applies_only_closed_and_is_bundled(self):
        prior = deepcopy(self.state["strategies"])
        self.state["pending_research"] = {"evaluated_at": "2026-10-12T13:30:00Z", "incumbent": prior,
            "results": {"swing": {"recommend_promote": True, "params": {"synthetic": 3}, "reason": "synthetic"}}}
        self.core.state = deepcopy(self.state)
        self.cycle()
        self.assertEqual(self.core.state["strategies"]["swing"]["params"], {"synthetic": 3})
        self.assertTrue((self.directory / "research.json").is_file())

    def test_first_risk_cycle_precedes_refresh_and_deferred_research(self):
        self.broker.open = True
        self.broker.now = datetime(2026, 10, 12, 11, 0, tzinfo=NY)
        calls = []
        original_account = self.broker.account
        self.broker.account = lambda: (calls.append("risk") or original_account())
        self.ops.refresh = lambda *args, **kwargs: (calls.append("refresh") or ({}, {}))
        self.ops.weekly_research_due = lambda *args: True

        def learn(state, daily, minute, directory, now, defer=False):
            self.assertTrue(defer)
            calls.append("learn")
            state["last_research"] = now.isoformat()
            state["pending_research"] = {"evaluated_at": now.isoformat(), "incumbent": deepcopy(state["strategies"]), "results": {}}
            write_json(directory / "research.json", {})
            return {}

        self.ops.learn = learn
        self.cycle()
        self.assertLess(calls.index("risk"), calls.index("refresh"))
        self.assertLess(calls.index("refresh"), calls.index("learn"))
        self.assertIn("pending_research", self.core.state)

    def test_mutation_then_local_research_error_is_saved_without_entry(self):
        self.broker.open = True
        self.broker.now = datetime(2026, 10, 12, 11, 0, tzinfo=NY)
        self.ops.weekly_research_due = lambda *args: True

        def learn(state, *args, **kwargs):
            state["last_research"] = "2026-10-12T15:00:00Z"
            raise OSError("SYNTHETIC private local path")

        self.ops.learn = learn
        result = self.cycle()
        self.assertEqual(self.core.state["last_research"], "2026-10-12T15:00:00Z")
        self.assertEqual(result["status"], "completed_with_alerts")
        self.assertEqual(self.broker.submitted, [])

    def test_collection_failure_blocks_entry_but_final_evidence_is_saved(self):
        self.broker.open = True
        self.ops.capture_fills = lambda *args: (_ for _ in ()).throw(OSError("SYNTHETIC"))
        self.ops.make_signals = lambda *args: {"swing": [{"symbol": "AAPL", "signal_time": self.broker.now.isoformat()}]}
        result = self.cycle()
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(result["status"], "completed_with_alerts")
        self.assertTrue(self.bundle.calls)

    def test_closed_lifecycle_collection_pause_matches_snapshot_archive_and_remote_core(self):
        # The first risk snapshot precedes collection. There is deliberately no
        # second engine cycle in this closed session to refresh its pause/alerts.
        self.broker.fill_activities = lambda **kwargs: []
        self.broker.options_lifecycle_activities = lambda **kwargs: [{
            "id": "synthetic-expiry-closed", "activity_type": "OPEXP",
            "date": "2026-10-14", "symbol": "AAPL261014C00100000", "qty": "-1"}]
        self.ops.capture_fills = capture_fills
        with patch("zerotrader.cloud_runner.CloudEngine.cycle", autospec=True,
                   side_effect=CloudEngine.cycle) as cycle, \
                patch.object(self.ops, "refresh", side_effect=AssertionError("closed session must not select")) as refresh:
            result = self.cycle()
        self.assertEqual(cycle.call_count, 1)
        refresh.assert_not_called()
        self.assertEqual(result["status"], "completed_with_alerts")
        self.assertTrue(result["paused"])
        self.assertEqual(self.broker.submitted, [])
        latest = json.loads((self.directory / "latest.json").read_text())
        with self.archive._connect() as db:
            run = db.execute("SELECT status,snapshot_hash FROM runs WHERE action='cloud_cycle'").fetchone()
            lifecycle_count = db.execute("SELECT count(*) FROM events WHERE kind='broker_option_lifecycle_activity'").fetchone()[0]
            fills_count = db.execute("SELECT count(*) FROM events WHERE kind='broker_fill_activity'").fetchone()[0]
        archived = self.archive.get_blob(run["snapshot_hash"])
        self.assertEqual(run["status"], "completed_with_alerts")
        self.assertEqual((lifecycle_count, fills_count), (1, 0))
        self.assertEqual(archived, latest)
        self.assertTrue(latest["paused"])
        self.assertTrue(any("lifecycle" in alert and "manual" in alert for alert in latest["alerts"]))
        self.assertTrue(any(event.get("event") == "options_lifecycle_manual_review" for event in latest["events"]))
        self.assertEqual(latest["paused"], self.core.state["paused"])
        self.assertEqual(latest["halted"], self.core.state["halted"])
        self.assertEqual(latest["events"], self.core.state["events"][-30:])
        self.assertTrue(any(review["manual_review_required"]
                            for review in self.core.state["strategies"]["options"]["lifecycle_reviews"].values()))
        self.assertEqual(self.bundle.calls[-1][1], self.core.state)

    def test_collection_mutation_checkpoint_failure_poison_stops_before_signal_cycle(self):
        self.broker.open = True
        self.broker.now = datetime(2026, 10, 14, 11, 0, tzinfo=NY)
        self.broker.fill_activities = lambda **kwargs: []
        self.broker.options_lifecycle_activities = lambda **kwargs: [{
            "id": "synthetic-expiry-uncertain-save", "activity_type": "OPEXP",
            "date": "2026-10-14", "symbol": "AAPL261014C00100000", "qty": "-1"}]
        self.ops.capture_fills = capture_fills
        failed_attempts = []

        def reject_mutated_pause(candidate):
            if candidate["paused"] and candidate["strategies"].get("options", {}).get("lifecycle_reviews"):
                failed_attempts.append(deepcopy(candidate))
                raise CheckpointUncertain(self.core.receipt)

        self.core.before = reject_mutated_pause
        with patch("zerotrader.cloud_runner.CloudEngine.cycle", autospec=True,
                   side_effect=CloudEngine.cycle) as cycle, \
                patch.object(self.ops, "refresh", side_effect=AssertionError("must stop before refresh")) as refresh, \
                patch.object(self.ops, "make_signals", side_effect=AssertionError("must not select entries")) as signals:
            with self.assertRaises(CloudRunError):
                self.cycle()
        self.assertEqual(len(failed_attempts), 1)
        self.assertEqual(cycle.call_count, 1)
        refresh.assert_not_called()
        signals.assert_not_called()
        self.assertTrue(self.coordinator.failed)
        self.assertTrue(self.state["paused"])
        self.assertFalse(self.core.state["paused"])
        self.assertNotIn("lifecycle_reviews", self.core.state["strategies"]["options"])
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.bundle.calls, [])
        self.assertFalse((self.directory / "latest.json").exists())
        with self.archive._connect() as db:
            self.assertEqual(db.execute("SELECT status FROM runs WHERE action='cloud_cycle'").fetchone()[0], "failed")
            self.assertEqual(db.execute("SELECT count(*) FROM events WHERE kind='cloud_collection_failed'").fetchone()[0], 0)
        # The persistence error must not be downgraded to collection degradation,
        # and the poisoned coordinator cannot attempt a later broker cycle.
        with self.assertRaises(CloudDurabilityError):
            self.cycle()
        self.assertEqual(len(failed_attempts), 1)

    def test_real_cloud_engine_buy_uses_shared_core_bundle_chain(self):
        self.broker.open = True
        self.broker.now = datetime(2026, 10, 14, 11, 0, tzinfo=NY)
        self.ops.make_signals = lambda *args: {"swing": [{"symbol": "AAPL", "signal_time": self.broker.now.isoformat()}]}
        self.cycle()
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.coordinator.receipt.commit, self.core.receipt.commit)
        self.assertGreaterEqual(len(self.bundle.calls), 4)

    def test_restore_uses_current_core_preserves_aux_and_starts_entry_disabled(self):
        capture_auxiliary(self.archive, self.run_id, self.directory)
        self.bundle.archive_path = self.archive.path
        destination = self.root / "cold"
        restored = CloudCoordinator.restore(self.core, self.bundle, destination, self.expected)
        self.assertEqual(restored.state, self.core.state)
        self.assertFalse(restored.evidence_ready)
        self.assertTrue((destination / "archive-backfill.json").is_file())
        self.assertEqual(json.loads((destination / "state.json").read_text()), self.core.state)

    def test_bad_cutover_and_account_hash_fail_before_broker(self):
        for key, bad in (("local_worker_stopped", False), ("local_logon_task_disabled", False),
                         ("branch", "other"), ("evidence_cutoff", True), ("source_commit", "not-a-commit"),
                         ("stopped_at", "2020-01-01T00:00:00"), ("account_sha256", "0" * 64)):
            with self.subTest(key=key):
                proof = dict(self.proof, **{key: bad})
                with self.assertRaises(CloudRunError):
                    validate_cutover(proof, self.state, self.expected)
        with self.assertRaises(CloudRunError):
            validate_cutover(self.proof, self.state, "0" * 64)

    def test_environment_requires_fixed_repo_main_manual_or_schedule_and_enabled(self):
        env = {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": CODE_REPOSITORY,
               "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch",
               "GITHUB_RUN_ID": "123", "ZOH_CLOUD_ENABLED": "true", "ZOH_EXPECTED_ACCOUNT_SHA256": self.expected}
        self.assertEqual(validate_environment(env), self.expected)
        self.assertEqual(validate_environment(dict(env, ZOH_EXPECTED_ACCOUNT_SHA256=self.expected + "\r\n")), self.expected)
        for key, bad in (("GITHUB_ACTIONS", "false"), ("GITHUB_REPOSITORY", "synthetic/other"),
                         ("GITHUB_REF", "refs/heads/other"), ("GITHUB_EVENT_NAME", "pull_request"),
                         ("ZOH_CLOUD_ENABLED", "false"), ("GITHUB_RUN_ID", "")):
            with self.subTest(key=key), self.assertRaises(CloudRunError):
                validate_environment(dict(env, **{key: bad}))

    def test_only_outer_ascii_credential_whitespace_is_normalized(self):
        with patch.dict("os.environ", {"ZOH_MAIN_PAPER_KEY": " SYNTHETIC_KEY\r\n",
                                       "ZOH_MAIN_PAPER_SECRET": "\tSYNTHETIC_SECRET\n"}):
            self.assertEqual(environment_paper_credentials(), {"key": "SYNTHETIC_KEY", "secret": "SYNTHETIC_SECRET"})
        for bad in ("SYNTHETIC KEY", "SYNTHETIC\nKEY", "\u00a0SYNTHETIC_KEY"):
            with patch.dict("os.environ", {"ZOH_MAIN_PAPER_KEY": bad, "ZOH_MAIN_PAPER_SECRET": "SYNTHETIC_SECRET"}):
                with self.assertRaises(CloudRunError):
                    environment_paper_credentials()

    def test_public_cli_errors_never_expose_exception_or_inputs(self):
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(["--duration-seconds", "SYNTHETIC_PRIVATE_VALUE"], environ={},
                        core_factory=lambda: self.fail("must not instantiate"))
        self.assertEqual(code, 1)
        self.assertEqual(errors.getvalue(), "")
        self.assertEqual(json.loads(output.getvalue()), {"mode": "PAPER", "status": "stopped",
            "new_entries": "blocked", "action": "restore_and_reconcile"})

    def test_duration_uses_natural_interval_and_has_no_cycle_retry(self):
        elapsed, calls = [0.0], []

        def cycle(*args):
            calls.append(elapsed[0])
            return {"market_open": True}

        fake = SimpleNamespace(run_cycle=cycle)
        result = run_duration(fake, object(), {}, 130, clock=lambda: elapsed[0],
                              sleeper=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
                              wall_clock=lambda: datetime(2026, 10, 14, 12, 0, tzinfo=NY))
        self.assertEqual(calls, [0.0, 60.0, 120.0])
        self.assertEqual(result["cycles"], 3)
        fake.run_cycle = lambda *args: (_ for _ in ()).throw(CloudRunError("synthetic"))
        with self.assertRaises(CloudRunError):
            run_duration(fake, object(), {}, 130, clock=lambda: elapsed[0], sleeper=lambda seconds: None,
                         wall_clock=lambda: datetime(2026, 10, 14, 12, 0, tzinfo=NY))

    def test_session_gate_and_boundary_exit(self):
        for day, hour, minute, expected in ((14, 8, 59, False), (14, 9, 0, True),
                (14, 17, 29, True), (14, 17, 30, False), (17, 11, 0, False)):
            self.assertEqual(within_session_window(datetime(2026, 10, day, hour, minute, tzinfo=NY)), expected)
        elapsed, calls = [0.0], []
        start = datetime(2026, 10, 14, 17, 29, 50, tzinfo=NY)
        fake = SimpleNamespace(run_cycle=lambda *args: (calls.append(True) or {"market_open": False}))
        run_duration(fake, object(), {}, 300, clock=lambda: elapsed[0],
                     sleeper=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
                     wall_clock=lambda: start + timedelta(seconds=elapsed[0]))
        self.assertEqual(len(calls), 1)
        self.assertEqual(elapsed[0], 10)

    def test_archived_collection_alert_does_not_count_as_healthy_recovery(self):
        elapsed = [0.0]
        fake = SimpleNamespace(run_cycle=lambda *args: {
            "market_open": True, "archive_complete": True, "status": "completed_with_alerts"})
        result = run_duration(fake, object(), {}, 130, clock=lambda: elapsed[0],
                              sleeper=lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds),
                              wall_clock=lambda: datetime(2026, 10, 14, 12, 0, tzinfo=NY))
        self.assertEqual(result["cycles"], 3)
        self.assertEqual(result["verified_cycles"], 0)
        self.assertEqual(result["status"], "completed_with_alerts")

    def test_outside_window_exits_before_restore_or_broker_creation(self):
        env = {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": CODE_REPOSITORY,
               "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "schedule",
               "GITHUB_RUN_ID": "123", "ZOH_CLOUD_ENABLED": "true", "ZOH_EXPECTED_ACCOUNT_SHA256": self.expected}
        output = io.StringIO()
        with redirect_stdout(output):
            code = main([], environ=env, core_factory=lambda: self.fail("must not instantiate"),
                        broker_factory=lambda **kwargs: self.fail("must not instantiate"),
                        wall_clock=lambda: datetime(2026, 10, 14, 17, 30, tzinfo=NY))
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue())["status"], "outside_session")

    def synthetic_cli_after_restore(self, restore_seconds, duration_seconds, *, start=None):
        elapsed, cycles, sleeps, broker_calls, closed = [0.0], [], [], [], []
        start = start or datetime(2026, 10, 14, 12, 0, tzinfo=NY)
        temporary, workspace = self.root / "runner-temp", self.root / "workspace"
        temporary.mkdir(exist_ok=True)
        workspace.mkdir(exist_ok=True)
        env = {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": CODE_REPOSITORY,
               "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "schedule",
               "GITHUB_RUN_ID": "123", "ZOH_CLOUD_ENABLED": "true",
               "ZOH_EXPECTED_ACCOUNT_SHA256": self.expected,
               "RUNNER_TEMP": str(temporary), "GITHUB_WORKSPACE": str(workspace)}

        def cycle(*args):
            cycles.append(elapsed[0])
            return {"market_open": True, "archive_complete": True, "status": "completed"}

        def restore(*args):
            elapsed[0] += restore_seconds
            return SimpleNamespace(run_cycle=cycle)

        def sleep(seconds):
            sleeps.append(seconds)
            elapsed[0] += seconds

        def broker(**kwargs):
            broker_calls.append(True)
            return object()

        output = io.StringIO()
        core = SimpleNamespace(close=lambda: closed.append(True))
        with patch.object(CloudCoordinator, "restore", side_effect=restore), redirect_stdout(output):
            code = main(["--duration-seconds", str(duration_seconds)], environ=env,
                        core_factory=lambda: core, bundle_factory=lambda core: nullcontext(object()),
                        broker_factory=broker, clock=lambda: elapsed[0], sleeper=sleep,
                        wall_clock=lambda: start + timedelta(seconds=elapsed[0]))
        self.assertEqual(code, 0)
        self.assertEqual(closed, [True])
        return json.loads(output.getvalue()), cycles, sleeps, broker_calls, elapsed[0]

    def test_cold_restore_exhausts_cli_budget_before_broker_or_cycle(self):
        for restore_seconds in (60, 61):
            with self.subTest(restore_seconds=restore_seconds):
                result, cycles, sleeps, brokers, elapsed = self.synthetic_cli_after_restore(restore_seconds, 60)
                self.assertEqual(result["status"], "budget_exhausted")
                self.assertEqual(result["cycles"], 0)
                self.assertEqual((cycles, sleeps, brokers), ([], [], []))
                self.assertEqual(elapsed, restore_seconds)

    def test_cold_restore_leaves_only_original_budget_remainder(self):
        result, cycles, sleeps, brokers, elapsed = self.synthetic_cli_after_restore(75, 130)
        self.assertEqual(result["cycles"], 1)
        self.assertEqual(result["verified_cycles"], 1)
        self.assertEqual(cycles, [75])
        self.assertEqual(sleeps, [55])
        self.assertEqual(brokers, [True])
        self.assertEqual(elapsed, 130)

    def test_restore_crossing_session_end_skips_broker_and_cycle(self):
        result, cycles, sleeps, brokers, elapsed = self.synthetic_cli_after_restore(
            20, 130, start=datetime(2026, 10, 14, 17, 29, 50, tzinfo=NY))
        self.assertEqual(result["status"], "outside_session")
        self.assertEqual(result["cycles"], 0)
        self.assertEqual((cycles, sleeps, brokers), ([], [], []))
        self.assertEqual(elapsed, 20)

    def test_soft_deadline_does_not_interrupt_final_active_cycle(self):
        elapsed, completed = [0.0], []

        def cycle(*args):
            elapsed[0] += 90
            completed.append("final_checkpoint_confirmed")
            return {"market_open": True, "archive_complete": True, "status": "completed"}

        result = run_duration(SimpleNamespace(run_cycle=cycle), object(), {}, 60,
                              clock=lambda: elapsed[0],
                              sleeper=lambda seconds: self.fail("no sleep after deadline"),
                              wall_clock=lambda: datetime(2026, 10, 14, 12, 0, tzinfo=NY))
        self.assertEqual(completed, ["final_checkpoint_confirmed"])
        self.assertEqual(result["verified_cycles"], 1)
        self.assertEqual(result["cycles"], 1)
        self.assertEqual(elapsed[0], 90)


if __name__ == "__main__":
    unittest.main()
