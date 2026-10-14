"""Synthetic native receipts and local bare Git; no host maintenance or network."""
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch

import test_bootstrap_private_state as baseline_tests
from zerotrader.cloud_checkpoint import _canonical
from zerotrader.cloud_runner import account_digest, validate_cutover
from zerotrader.storage import write_json

SCRIPT = Path(__file__).resolve().parents[1] / "cloud-main/prepare_cloud_cutover.py"
SPEC = importlib.util.spec_from_file_location("zoh_test_cloud_cutover", SCRIPT)
cutover = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cutover)


class CloudCutoverPreparationTests(unittest.TestCase):
    def setUp(self):
        # Reuse only fixture construction, not inherited tests or live paths.
        baseline_tests.BootstrapPrivateStateTests.setUp(self)
        self.state["known_order_ids"] = ["synthetic-parent", "synthetic-stop-child"]
        self.state["pending_research"] = {"synthetic_candidate": {"unchanged": True}}
        self.state["intents"]["synthetic-persisted-uncertain-intent"]["order_id"] = "synthetic-parent"
        write_json(self.runtime / "state.json", self.state)
        self.snapshot = self.root / "fresh-stopped-snapshot"
        self.digest = cutover._snapshot.prepare_snapshot(self.runtime, self.snapshot)["manifest_sha256"]
        self.manifest = json.loads((self.snapshot / "manifest.json").read_bytes())
        started = datetime.fromisoformat(self.manifest["capture_started_at"])
        verified = datetime.fromisoformat(self.manifest["verified_at"])
        self.now = verified + timedelta(seconds=2)
        self.runtime_binding, self.code_commit = "b" * 64, "c" * 40
        self.account_hash = account_digest(self.state)
        self.native = self.root / "synthetic-native-evidence.json"
        self.evidence = {
            "version": 1, "kind": "native_cloud_cutover_evidence",
            "runtime_binding_sha256": self.runtime_binding, "account_sha256": self.account_hash,
            "code_repository": cutover.CODE_REPOSITORY, "code_commit": self.code_commit,
            "snapshot_manifest_sha256": self.digest, "core_state_sha256": self.manifest["core_state_sha256"],
            "task_registered": True, "task_identity_verified": True, "task_enabled": False,
            "stop_requested": True, "stop_flag_present": True, "verified_process_count": 0,
            "active_child_process_count": 0, "stopped_at": (started - timedelta(seconds=2)).isoformat(),
            "disabled_at": (started - timedelta(seconds=1)).isoformat(),
            "observed_at": (verified + timedelta(seconds=1)).isoformat(),
        }
        self.write_evidence()

    def write_evidence(self):
        self.native.write_bytes(_canonical(self.evidence))
        self.evidence_hash = hashlib.sha256(self.native.read_bytes()).hexdigest()

    def prepare(self, **changes):
        args = dict(snapshot=self.snapshot, destination=self.destination, manifest_sha256=self.digest,
                    native_evidence=self.native, native_evidence_sha256=self.evidence_hash,
                    runtime_binding_sha256=self.runtime_binding, account_sha256=self.account_hash,
                    code_commit=self.code_commit, now=self.now)
        args.update(changes)
        return cutover.prepare_cloud_cutover(**args)

    def assert_refused_without_destination(self, **changes):
        with self.assertRaises(cutover.CutoverPreparationError):
            self.prepare(**changes)
        self.assertFalse(self.destination.exists())

    @unittest.skipUnless(shutil.which("git"), "Local Git is required")
    def test_production_branch_roundtrip_preserves_risk_cids_evidence_and_source(self):
        before = {str(p.relative_to(self.snapshot)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in self.snapshot.rglob("*") if p.is_file()}
        with patch.object(cutover, "GitCheckpointStore", wraps=cutover.GitCheckpointStore) as stores:
            result = self.prepare()
        self.assertEqual(stores.call_count, 2)
        self.assertTrue(all(call.kwargs == {"branch": "core-checkpoints", "timeout_seconds": 180}
                            for call in stores.call_args_list))
        self.assertEqual(result["status"], "verified_local_cutover_baseline")
        self.assertEqual(result["branch"], "core-checkpoints")
        self.assertFalse(result["trading_enabled"])
        self.assertFalse(result["remote_published"])
        self.assertTrue(result["requires_final_native_recheck"])
        restored = self.destination / "verified-restore"
        state = json.loads((restored / "state.json").read_bytes())
        self.assertEqual(state, self.state)
        self.assertTrue(state["paused"])
        self.assertEqual(state["halted"], "synthetic latched risk")
        self.assertEqual(state["equity_peak"], 103000.0)
        self.assertEqual(state["intents"]["synthetic-persisted-uncertain-intent"]["status"], "uncertain")
        self.assertEqual(state["known_order_ids"], ["synthetic-parent", "synthetic-stop-child"])
        self.assertEqual(state["pending_research"], self.state["pending_research"])
        proof = json.loads((restored / "cloud-cutover.json").read_bytes())
        self.assertTrue(validate_cutover(proof, state, self.account_hash, now=self.now))
        self.assertEqual(proof["evidence_cutoff"], self.manifest["archive"]["evidence_cutoff_sequence"]["events"])
        self.assertEqual(result["proof_sha256"], hashlib.sha256((restored / "cloud-cutover.json").read_bytes()).hexdigest())
        receipt_file = restored / ("reviews/cloud-cutover-native-evidence-" + self.evidence_hash + ".json")
        self.assertEqual(receipt_file.read_bytes(), self.native.read_bytes())
        self.assertEqual(result["fill_counts"], {"raw_fill_event_rows": 2, "raw_fill_versions": 2,
            "distinct_fill_activity_ids": 1, "correction_versions": 1})
        self.assertEqual(result["prepared_table_counts"], result["restored_table_counts"])
        self.assertEqual(result["excluded_historical_attachment_count"], 2)
        self.assertEqual((restored / "monitor-state.json").read_bytes(), (self.snapshot / "monitor-state.json").read_bytes())
        after = {str(p.relative_to(self.snapshot)): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in self.snapshot.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        saved = json.loads((self.destination / "verified-cutover-baseline.json").read_bytes())
        self.assertEqual(saved, result)
        for private in (self.state["account_id"], self.account_hash, self.runtime_binding,
                        str(self.root), "synthetic-persisted-uncertain-intent"):
            self.assertNotIn(private, json.dumps(result))

    def test_native_hash_or_snapshot_hash_mismatch_refused(self):
        self.assert_refused_without_destination(native_evidence_sha256="0" * 64)
        self.assert_refused_without_destination(manifest_sha256="0" * 64)

    @unittest.skipUnless(shutil.which("git"), "Local Git is required")
    def test_failed_fresh_restore_does_not_claim_verified_baseline_or_leak_error(self):
        original = (self.snapshot / "state.json").read_bytes()
        with patch.object(cutover.GitBundleStore, "restore",
                          side_effect=RuntimeError("SYNTHETIC_PRIVATE_FAILURE")):
            with self.assertRaises(cutover.CutoverPreparationError) as failure:
                self.prepare()
        self.assertEqual(str(failure.exception), "local_cutover_preparation_failed")
        self.assertTrue((self.destination / "baseline.git").is_dir())
        self.assertFalse((self.destination / "verified-cutover-baseline.json").exists())
        self.assertEqual((self.snapshot / "state.json").read_bytes(), original)

    def test_caller_runtime_account_and_reviewed_code_must_match(self):
        for args in ({"runtime_binding_sha256": "0" * 64}, {"account_sha256": "0" * 64},
                     {"code_commit": "0" * 40}):
            with self.subTest(args=args):
                self.assert_refused_without_destination(**args)

    def test_native_bindings_cannot_be_substituted_even_with_new_file_hash(self):
        original = deepcopy(self.evidence)
        for field, value in (("snapshot_manifest_sha256", "0" * 64), ("core_state_sha256", "0" * 64),
                             ("code_repository", "synthetic/other")):
            with self.subTest(field=field):
                self.evidence = dict(original, **{field: value})
                self.write_evidence()
                self.assert_refused_without_destination()

    def test_enabled_task_unverified_identity_stop_absence_and_active_process_refused(self):
        original = deepcopy(self.evidence)
        for field, value in (("task_registered", False), ("task_identity_verified", False),
                ("task_enabled", True), ("stop_requested", False), ("stop_flag_present", False),
                ("verified_process_count", 2), ("active_child_process_count", 1),
                ("verified_process_count", False), ("active_child_process_count", 0.0)):
            with self.subTest(field=field, value=value):
                self.evidence = dict(original, **{field: value})
                self.write_evidence()
                self.assert_refused_without_destination()

    def test_receipt_boolean_cannot_replace_required_pinned_evidence_schema(self):
        self.evidence["native_source_verified"] = True
        self.write_evidence()
        self.assert_refused_without_destination()

    def test_post_capture_stop_future_observation_and_naive_time_refused(self):
        original = deepcopy(self.evidence)
        for field, value in (("stopped_at", self.now.isoformat()), ("disabled_at", self.now.isoformat()),
                ("observed_at", (self.now + timedelta(seconds=1)).isoformat()),
                ("observed_at", self.now.replace(tzinfo=None).isoformat()),
                ("observed_at", self.evidence["stopped_at"])):
            with self.subTest(field=field, value=value):
                self.evidence = dict(original, **{field: value})
                self.write_evidence()
                self.assert_refused_without_destination()

    def test_stale_native_evidence_refused(self):
        self.assert_refused_without_destination(now=self.now + timedelta(seconds=301))

    def test_fresh_native_observation_does_not_allow_stale_snapshot(self):
        later = self.now + timedelta(seconds=901)
        self.evidence["observed_at"] = later.isoformat()
        self.write_evidence()
        self.assert_refused_without_destination(now=later)

    def test_bad_output_locations_and_preexisting_output_refused(self):
        for path in (self.snapshot, self.snapshot / "nested", SCRIPT.parent / "must-not-create"):
            with self.subTest(path=path):
                self.assert_refused_without_destination(destination=path)

    def test_cli_errors_are_fixed_and_do_not_echo_inputs(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = cutover.main(["SYNTHETIC_PRIVATE_INPUT"])
        self.assertEqual(code, 1)
        self.assertNotIn("SYNTHETIC_PRIVATE_INPUT", output.getvalue())
        self.assertEqual(json.loads(output.getvalue())["error_category"], "invalid_arguments")


if __name__ == "__main__":
    unittest.main()
