"""Synthetic private runtime snapshots only. No broker, credentials or network."""
from contextlib import closing, redirect_stdout
from copy import deepcopy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from zerotrader.archive import ResearchArchive
from zerotrader.archival import fingerprint
from zerotrader.storage import process_lock


_SPEC = importlib.util.spec_from_file_location(
    "prepare_migration_snapshot", Path(__file__).resolve().parents[1] / "cloud-main" / "prepare_migration_snapshot.py")
snapshot = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(snapshot)


def state():
    cid = "synthetic-original-client-id"
    return {"version": 1, "account_id": "synthetic-private-account", "equity_peak": 103000.,
            "halted": "synthetic latched halt", "paused": True,
            "owners": {"SYN": {"sleeve": "swing", "entry_cid": cid}},
            "intents": {cid: {"payload": {"client_order_id": cid, "symbol": "SYN", "side": "buy", "qty": "1"},
                              "sleeve": "swing", "status": "uncertain", "created": "2026-01-02T15:00:00Z"}},
            "fills": {}, "strategies": {}, "events": [], "last_cycle": None, "last_research": None}


class MigrationSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="zoh-synthetic-snapshot-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.destination = self.root / "validation-copy"
        (self.runtime / "runner.lock").write_bytes(b"0")
        self.state = state()
        (self.runtime / "state.json").write_text(json.dumps(self.state), encoding="utf-8")
        self.application = self.root / "source-fixture"
        (self.application / "zerotrader").mkdir(parents=True)
        (self.application / "zerotrader" / "engine.py").write_text("SYNTHETIC = True\n", encoding="utf-8")
        source = patch("zerotrader.archive.ROOT", self.application)
        source.start()
        self.addCleanup(source.stop)
        self.archive = ResearchArchive(self.runtime)
        self.run = self.archive.start_run("synthetic", {"capital": 100000}, {})
        self.archive.dataset(self.run, "synthetic-bars", {"SYN": [{"c": 10.0}]})
        self.archive.samples(self.run, [{"symbol": "SYN", "sleeve": "swing", "decision": "not_selected"}])
        for identity, price in (("synthetic-fill-1", "10"), ("synthetic-fill-1", "10.1"), ("synthetic-fill-2", "11")):
            activity = {"id": identity, "activity_type": "FILL", "order_id": "synthetic-order", "qty": "1", "price": price}
            self.archive.event(self.run, "broker_fill_activity", {
                "source": "alpaca_account_activities_FILL", "activity_id": identity,
                "activity_hash": fingerprint(activity), "activity": activity})
        (self.runtime / "fill-archive-cursor.json").write_text('{"last_successful_poll":"2026-01-02T15:30:00Z"}')
        (self.runtime / "reviews").mkdir()
        (self.runtime / "reviews" / "synthetic.md").write_text("Synthetic private review\n")
        (self.runtime / "research-history").mkdir()
        (self.runtime / "research-history" / "synthetic.json").write_text('{"result":"rejected"}')

    def prepare(self):
        return snapshot.prepare_snapshot(self.runtime, self.destination)

    def test_copy_preserves_original_ids_risk_and_fill_correction_versions(self):
        original_state = (self.runtime / "state.json").read_bytes()
        original_archive = self.archive.path.read_bytes()
        result = self.prepare()
        self.assertEqual(result["status"], "prepared")
        self.assertFalse(result["cutover_proof"])
        self.assertFalse(result["trading_enabled"])
        self.assertEqual((self.destination / "state.json").read_bytes(), original_state)
        self.assertEqual((self.runtime / "state.json").read_bytes(), original_state)
        self.assertEqual(self.archive.path.read_bytes(), original_archive)
        self.assertFalse((self.destination / "archive/research.sqlite3-wal").exists())
        self.assertFalse((self.destination / "archive/research.sqlite3-shm").exists())
        manifest_raw = (self.destination / "manifest.json").read_bytes()
        self.assertEqual(hashlib.sha256(manifest_raw).hexdigest(), result["manifest_sha256"])
        manifest = json.loads(manifest_raw)
        self.assertEqual(manifest["snapshot_kind"], "readonly_validation")
        self.assertTrue(manifest["fresh_cutover_snapshot_required"])
        self.assertFalse(manifest["auxiliary_atomic_with_archive"])
        self.assertEqual(manifest["archive"]["raw_fill_event_rows"], 3)
        self.assertEqual(manifest["archive"]["raw_fill_versions"], 3)
        self.assertEqual(manifest["archive"]["distinct_fill_activity_ids"], 2)
        self.assertEqual(manifest["archive"]["correction_versions"], 1)
        for entry in manifest["files"]:
            raw = (self.destination / entry["path"]).read_bytes()
            self.assertEqual(len(raw), entry["bytes"])
            self.assertEqual(hashlib.sha256(raw).hexdigest(), entry["sha256"])
        self.assertNotIn(str(self.runtime), manifest_raw.decode())

    def test_busy_lock_returns_pending_without_creating_destination_or_reading_state(self):
        with process_lock(self.runtime), patch.object(snapshot, "_state_bytes") as read:
            result = self.prepare()
            read.assert_not_called()
        self.assertEqual(result["status"], "pending_lock")
        self.assertFalse(self.destination.exists())

    def test_credentials_exports_worker_logs_and_unknown_files_are_never_read_or_copied(self):
        forbidden = ("credentials.json", "worker.json", "worker-last-output.json", "unrelated.txt", "exports/private.csv")
        for name in forbidden:
            path = self.runtime / name
            path.parent.mkdir(exist_ok=True)
            path.write_text("DO NOT READ SYNTHETIC PRIVATE DATA")
        original = Path.open

        def guarded(path, *args, **kwargs):
            if path in {self.runtime / name for name in forbidden}:
                raise AssertionError("Forbidden path read")
            return original(path, *args, **kwargs)

        with patch.object(Path, "open", guarded), patch("socket.create_connection", side_effect=AssertionError("No network")), \
                patch("subprocess.run", side_effect=AssertionError("No subprocess")):
            result = self.prepare()
        self.assertEqual(result["status"], "prepared")
        for name in forbidden:
            self.assertFalse((self.destination / name).exists())

    def test_explicit_auxiliary_exclusions_are_disclosed_without_reading_or_deleting(self):
        name = "reviews/peer-code-synthetic/review.txt"
        excluded = self.runtime / name
        excluded.parent.mkdir()
        excluded.write_bytes(b"synthetic retained attachment")
        original = Path.open

        def guarded(path, *args, **kwargs):
            if path == excluded:
                raise AssertionError("Excluded attachment must not be read")
            return original(path, *args, **kwargs)

        with patch.object(Path, "open", guarded):
            self.prepare()
        manifest = json.loads((self.destination / "manifest.json").read_bytes())
        self.assertEqual(manifest["auxiliary_excluded"], [{
            "path": name, "reason": "historical_peer_source_attachment_retained_locally"}])
        self.assertTrue(excluded.exists())
        self.assertFalse((self.destination / name).exists())

    def test_partial_core_is_not_defaulted_or_completed(self):
        (self.runtime / "state.json").write_text('{"account_id":"synthetic"}')
        with self.assertRaisesRegex(snapshot.SnapshotError, "core_state_invalid"):
            self.prepare()
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_credential_field_in_state_or_auxiliary_fails_without_completion_manifest(self):
        value = deepcopy(self.state)
        value["strategies"]["api_key"] = "synthetic-secret"
        (self.runtime / "state.json").write_text(json.dumps(value))
        with self.assertRaises(snapshot.SnapshotError):
            self.prepare()
        self.assertFalse((self.destination / "manifest.json").exists())
        (self.runtime / "state.json").write_text(json.dumps(self.state))
        (self.runtime / "research.json").write_text('{"token":"synthetic-secret"}')
        self.destination = self.root / "second-invalid-copy"
        with self.assertRaises(snapshot.SnapshotError):
            self.prepare()
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_existing_destination_is_not_overwritten(self):
        self.destination.mkdir()
        sentinel = self.destination / "keep.txt"
        sentinel.write_text("keep")
        with self.assertRaisesRegex(snapshot.SnapshotError, "new_destination_required"):
            self.prepare()
        self.assertEqual(sentinel.read_text(), "keep")

    def test_missing_lock_never_initializes_runtime_or_snapshot(self):
        (self.runtime / "runner.lock").unlink()
        with self.assertRaises(snapshot.SnapshotError):
            self.prepare()
        self.assertFalse((self.runtime / "runner.lock").exists())
        self.assertFalse(self.destination.exists())

    def test_destination_inside_runtime_or_code_tree_is_refused(self):
        for target in (self.runtime / "copy", snapshot._SOURCE_ROOT / "synthetic-private-copy"):
            with self.subTest(target_type=target.name), self.assertRaises(snapshot.SnapshotError):
                snapshot.prepare_snapshot(self.runtime, target)
            self.assertFalse(target.exists())

    def test_linked_allowed_file_is_refused(self):
        target = self.root / "other-synthetic.json"
        target.write_text('{"safe":"outside"}')
        try:
            (self.runtime / "research.json").symlink_to(target)
        except OSError:
            self.skipTest("Symlinks unavailable")
        with self.assertRaises(snapshot.SnapshotError):
            self.prepare()
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_auxiliary_change_during_capture_is_detected(self):
        original = snapshot.cloud_auxiliary._read_safe
        calls = []

        def changed(root, name):
            raw = original(root, name)
            calls.append(name)
            if name == "fill-archive-cursor.json" and calls.count(name) == 2:
                return raw + b" "
            return raw

        with patch.object(snapshot.cloud_auxiliary, "_read_safe", changed), self.assertRaisesRegex(
                snapshot.SnapshotError, "source_changed_during_snapshot"):
            self.prepare()
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_invalid_fill_version_hash_does_not_receive_manifest(self):
        activity = {"id": "synthetic-bad-fill", "activity_type": "FILL", "qty": "1"}
        self.archive.event(self.run, "broker_fill_activity", {
            "source": "alpaca_account_activities_FILL", "activity_id": activity["id"],
            "activity_hash": "0" * 64, "activity": activity})
        with self.assertRaisesRegex(snapshot.SnapshotError, "raw_fill_evidence_invalid"):
            self.prepare()
        self.assertFalse((self.destination / "manifest.json").exists())

    def test_sqlite_backup_includes_uncheckpointed_wal(self):
        db = sqlite3.connect(self.archive.path)
        self.addCleanup(db.close)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA wal_autocheckpoint=0")
        self.archive.event(self.run, "synthetic_wal_event", {"evidence": "only in latest WAL"})
        self.assertTrue(Path(str(self.archive.path) + "-wal").exists())
        self.prepare()
        with closing(sqlite3.connect(self.destination / "archive" / "research.sqlite3")) as copied:
            self.assertEqual(copied.execute("SELECT COUNT(*) FROM events WHERE kind='synthetic_wal_event'").fetchone()[0], 1)

    def test_failure_releases_runner_lock_and_does_not_publish_partial_manifest(self):
        with patch.object(snapshot, "_backup", side_effect=OSError("synthetic-private-path-and-content")):
            with self.assertRaises(snapshot.SnapshotError) as raised:
                self.prepare()
        self.assertEqual(str(raised.exception), "snapshot_preparation_failed")
        self.assertFalse((self.destination / "manifest.json").exists())
        with process_lock(self.runtime):
            pass

    def test_cli_uses_fixed_errors_and_does_not_echo_private_argument(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = snapshot.main(["synthetic-private-argument"])
        self.assertEqual(code, 1)
        self.assertNotIn("synthetic-private-argument", output.getvalue())
        self.assertEqual(json.loads(output.getvalue())["error_category"], "invalid_arguments")


if __name__ == "__main__":
    unittest.main()
