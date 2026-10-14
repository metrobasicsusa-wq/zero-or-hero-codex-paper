"""Offline baseline helper with synthetic archives and temporary local Git."""
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from zerotrader.archive import ResearchArchive
from zerotrader.archival import fingerprint
from zerotrader.engine import new_state
from zerotrader.storage import write_json


SCRIPT = Path(__file__).resolve().parents[1] / "cloud-main/bootstrap_private_state.py"
SPEC = importlib.util.spec_from_file_location("zoh_test_offline_bootstrap", SCRIPT)
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)


class BootstrapPrivateStateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="zoh-synthetic-offline-baseline-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        app = self.root / "app"
        (app / "zerotrader").mkdir(parents=True)
        (app / "zerotrader" / "synthetic.py").write_text("SYNTHETIC = True\n")
        source = patch("zerotrader.archive.ROOT", app)
        source.start()
        self.addCleanup(source.stop)
        self.runtime = self.root / "synthetic-runtime"
        self.archive = ResearchArchive(self.runtime)
        self.run = self.archive.start_run("synthetic_paper_record", {}, {})
        self.state = new_state()
        self.state["account_id"] = "synthetic-account-never-a-broker"
        self.state.update(paused=True, halted="synthetic latched risk", equity_peak=103000.0)
        cid = "synthetic-persisted-uncertain-intent"
        self.state["intents"][cid] = {
            "payload": {"client_order_id": cid, "symbol": "SYN", "side": "buy", "qty": "2"},
            "sleeve": "swing", "status": "uncertain", "created": "2026-01-02T15:00:00Z"}
        self.state["owners"]["SYN"] = {"sleeve": "swing", "entry_cid": cid}
        write_json(self.runtime / "state.json", self.state)
        (self.runtime / "runner.lock").write_bytes(b"0")
        write_json(self.runtime / "archive-backfill.json", {"synthetic": True})
        write_json(self.runtime / "fill-archive-cursor.json", {"last_successful_poll": "2026-01-02T21:00:00Z"})
        write_json(self.runtime / "monitor-state.json", {"daily_reviews": {"2026-01-02": {"status": "complete"}}})
        (self.runtime / "reviews").mkdir()
        (self.runtime / "reviews/synthetic.txt").write_text("Synthetic audit; no real trades.\n")
        excluded = self.runtime / "reviews/claude-small-account-review-20261002-164726.json"
        excluded.write_text('{"authorization":{"synthetic":true}}')
        excluded_source = self.runtime / "reviews/peer-code-synthetic/package/__init__.py"
        excluded_source.parent.mkdir(parents=True)
        excluded_source.write_bytes(b"Must remain excluded; never import or archive its contents.")
        for price in ("10.01", "10.02"):
            activity = {"id": "synthetic-fill", "order_id": "synthetic-order", "activity_type": "FILL",
                        "symbol": "SYN", "qty": "2", "price": price, "side": "buy",
                        "transaction_time": "2026-01-02T15:00:00Z"}
            self.archive.event(self.run, "broker_fill_activity", {"source": "alpaca_account_activities_FILL",
                "activity_id": activity["id"], "activity_hash": fingerprint(activity), "activity": activity})
        self.archive.samples(self.run, [{"symbol": "SYN", "sleeve": "swing", "decision": "not_selected"}])
        self.archive.dataset(self.run, "synthetic", {"SYN": []})
        self.archive.finish_run(self.run, "completed", {"synthetic": True})
        self.snapshot = self.root / "snapshot"
        prepared = bootstrap._snapshot.prepare_snapshot(self.runtime, self.snapshot)
        self.digest = prepared["manifest_sha256"]
        self.destination = self.root / "baseline"

    @unittest.skipUnless(shutil.which("git"), "Local Git is required")
    def test_complete_baseline_fresh_restore_preserves_source_fills_aux_and_no_cutover(self):
        # Earlier snapshot-helper versions can leave harmless read-only SQLite
        # bookkeeping. These exact bytes must also remain untouched.
        (self.snapshot / "archive/research.sqlite3-wal").write_bytes(b"")
        (self.snapshot / "archive/research.sqlite3-shm").write_bytes(b"synthetic unused bookkeeping")
        before = {str(p.relative_to(self.snapshot)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in self.snapshot.rglob("*") if p.is_file()}
        with patch.object(bootstrap, "GitCheckpointStore", wraps=bootstrap.GitCheckpointStore) as stores:
            result = bootstrap.prepare_private_baseline(self.snapshot, self.destination, self.digest)
        self.assertEqual(len(stores.call_args_list), 2)
        self.assertTrue(all(call.kwargs["timeout_seconds"] == 180 for call in stores.call_args_list))
        self.assertEqual(result["status"], "verified")
        self.assertFalse(result["trading_enabled"])
        self.assertFalse(result["cutover_proof"])
        self.assertEqual(result["branch"], "offline-validation")
        self.assertEqual(result["generation"], 0)
        self.assertEqual(result["fill_counts"], {"raw_fill_event_rows": 2, "raw_fill_versions": 2,
            "distinct_fill_activity_ids": 1, "correction_versions": 1})
        self.assertEqual(result["prepared_table_counts"], result["restored_table_counts"])
        self.assertEqual(result["excluded_historical_attachment_count"], 2)
        restored = self.destination / "verified-restore"
        self.assertEqual(json.loads((restored / "state.json").read_text()), self.state)
        self.assertFalse((restored / "cloud-cutover.json").exists())
        self.assertFalse((restored / "reviews/peer-code-synthetic/package/__init__.py").exists())
        self.assertEqual((restored / "reviews/synthetic.txt").read_bytes(), (self.snapshot / "reviews/synthetic.txt").read_bytes())
        self.assertTrue((self.destination / "verified-baseline.json").is_file())
        after = {str(p.relative_to(self.snapshot)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in self.snapshot.rglob("*") if p.is_file()}
        self.assertEqual(after, before)
        serialized = json.dumps(result)
        self.assertNotIn(self.state["account_id"], serialized)
        self.assertNotIn(str(self.root), serialized)

    def test_wrong_reviewed_manifest_hash_refuses_before_output_creation(self):
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.prepare_private_baseline(self.snapshot, self.destination, "0" * 64)
        self.assertFalse(self.destination.exists())

    def test_modified_source_file_refuses_before_output_creation(self):
        (self.snapshot / "reviews/synthetic.txt").write_text("changed")
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.prepare_private_baseline(self.snapshot, self.destination, self.digest)
        self.assertFalse(self.destination.exists())

    def test_cutover_proof_or_live_runtime_marker_is_refused(self):
        for name in ("cloud-cutover.json", "runner.lock", "worker.json"):
            with self.subTest(name=name):
                path = self.snapshot / name
                path.write_bytes(b"synthetic")
                with self.assertRaises(bootstrap.BootstrapError):
                    bootstrap.prepare_private_baseline(self.snapshot, self.destination, self.digest)
                path.unlink()
        self.assertFalse(self.destination.exists())

    def test_nonempty_wal_is_refused_before_output_creation(self):
        (self.snapshot / "archive/research.sqlite3-wal").write_bytes(b"uncheckpointed synthetic evidence")
        with self.assertRaises(bootstrap.BootstrapError):
            bootstrap.prepare_private_baseline(self.snapshot, self.destination, self.digest)
        self.assertFalse(self.destination.exists())

    def test_manifest_traversal_or_false_fill_statistics_are_refused(self):
        original = (self.snapshot / "manifest.json").read_bytes()
        for mutation in ("path", "fills"):
            manifest = json.loads(original)
            if mutation == "path":
                manifest["files"][-1]["path"] = "../not-allowed.txt"
            else:
                manifest["archive"]["raw_fill_versions"] += 1
            changed = json.dumps(manifest, sort_keys=True).encode()
            (self.snapshot / "manifest.json").write_bytes(changed)
            with self.assertRaises(bootstrap.BootstrapError):
                bootstrap.prepare_private_baseline(self.snapshot, self.destination, hashlib.sha256(changed).hexdigest())
        self.assertFalse(self.destination.exists())

    def test_exclusion_traversal_or_unknown_category_refused_before_output(self):
        original = (self.snapshot / "manifest.json").read_bytes()
        reason = "historical_peer_source_attachment_retained_locally"
        for name in ("reviews/peer-code-synthetic/../outside.py",
                     "/reviews/peer-code-synthetic/__init__.py",
                     "reviews/not-reviewed/__init__.py"):
            with self.subTest(name=name):
                manifest = json.loads(original)
                manifest["auxiliary_excluded"] = [{"path": name, "reason": reason}]
                changed = json.dumps(manifest, sort_keys=True).encode()
                (self.snapshot / "manifest.json").write_bytes(changed)
                with self.assertRaises(bootstrap.BootstrapError):
                    bootstrap.prepare_private_baseline(
                        self.snapshot, self.destination, hashlib.sha256(changed).hexdigest())
                self.assertFalse(self.destination.exists())

    def test_outputs_inside_source_or_project_and_existing_destination_refused(self):
        for destination in (self.snapshot / "nested", SCRIPT.parent / "synthetic-must-not-create", self.snapshot):
            with self.subTest(destination=destination), self.assertRaises(bootstrap.BootstrapError):
                bootstrap.prepare_private_baseline(self.snapshot, destination, self.digest)

    def test_cli_fixed_failure_does_not_print_input_paths(self):
        output = io.StringIO()
        with redirect_stdout(output):
            result = bootstrap.main(["--snapshot", "SYNTHETIC_PRIVATE_PATH", "--destination", str(self.destination),
                                     "--manifest-sha256", self.digest])
        self.assertEqual(result, 1)
        self.assertNotIn("SYNTHETIC_PRIVATE_PATH", output.getvalue())
        self.assertFalse(json.loads(output.getvalue())["trading_enabled"])


if __name__ == "__main__":
    unittest.main()
