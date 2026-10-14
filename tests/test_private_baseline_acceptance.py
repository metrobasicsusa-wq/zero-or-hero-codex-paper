"""Synthetic private baseline acceptance tests. No broker or real network."""
from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

from zerotrader.archive import ResearchArchive
from zerotrader.archival import fingerprint
from zerotrader.cloud_checkpoint import CHECKPOINT_PATH, GitCheckpointStore, _canonical
from zerotrader.engine import new_state
from zerotrader.storage import write_json


def load_script(name, filename):
    script = Path(__file__).resolve().parents[1] / "cloud-main" / filename
    spec = importlib.util.spec_from_file_location(name, script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


acceptance = load_script("zoh_acceptance_synthetic", "verify_private_baseline.py")
bootstrap = load_script("zoh_baseline_synthetic", "bootstrap_private_state.py")


class SyntheticReadStore(acceptance.ReadOnlyOfflineStore):
    def __init__(self, bare):
        GitCheckpointStore.__init__(self, bare, branch=acceptance.BRANCH)

    def _git_environment(self):
        return GitCheckpointStore._git_environment(self)

    def verify_private_repository(self):
        return True  # local temporary fixture; never a claim about GitHub


@unittest.skipUnless(shutil.which("git"), "Local Git required")
class PrivateBaselineAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="zoh-synthetic-acceptance-")
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.app = cls.root / "synthetic-app"
        (cls.app / "zerotrader").mkdir(parents=True)
        (cls.app / "zerotrader/synthetic.py").write_text("SYNTHETIC_ONLY = True\n")
        source = patch("zerotrader.archive.ROOT", cls.app)
        source.start()
        cls.addClassCleanup(source.stop)
        runtime = cls.root / "runtime"
        archive = ResearchArchive(runtime)
        run = archive.start_run("synthetic", {}, {})
        cls.state = new_state()
        cls.state.update(account_id="synthetic-private-account", paused=True,
                         halted="synthetic-preserved-risk", equity_peak=103000.0)
        cid = "synthetic-persisted-original-intent"
        cls.state["intents"][cid] = {"payload": {"client_order_id": cid, "symbol": "SYN", "side": "buy", "qty": "2"},
                                    "sleeve": "swing", "status": "uncertain", "created": "2026-01-02T15:00:00Z"}
        cls.state["owners"]["SYN"] = {"sleeve": "swing", "entry_cid": cid}
        cls.account_hash = hashlib.sha256(cls.state["account_id"].encode()).hexdigest()
        write_json(runtime / "state.json", cls.state)
        (runtime / "runner.lock").write_bytes(b"0")
        write_json(runtime / "monitor-state.json", {"synthetic": True})
        write_json(runtime / "fill-archive-cursor.json", {"last_successful_poll": "2026-01-02T21:00:00Z"})
        for price in ("10.00", "10.05"):
            activity = {"id": "synthetic-activity", "activity_type": "FILL", "order_id": "synthetic-order", "price": price, "qty": "2"}
            archive.event(run, "broker_fill_activity", {"source": "alpaca_account_activities_FILL",
                "activity_id": activity["id"], "activity_hash": fingerprint(activity), "activity": activity})
        archive.samples(run, [{"symbol": "SYN", "decision": "not_selected", "sleeve": "swing"}])
        archive.dataset(run, "synthetic", {"SYN": []})
        archive.finish_run(run, "completed")
        snapshot = cls.root / "snapshot"
        prepared = bootstrap._snapshot.prepare_snapshot(runtime, snapshot)
        cls.source_hash = prepared["manifest_sha256"]
        cls.manifest_raw = (snapshot / "manifest.json").read_bytes()
        cls.manifest = json.loads(cls.manifest_raw)
        baseline = cls.root / "baseline"
        result = bootstrap.prepare_private_baseline(snapshot, baseline, cls.source_hash)
        cls.working = baseline / "working-runtime"
        cls.bare = baseline / "baseline.git"
        # A local synthetic append mirrors the parent's separate private upload:
        # updating the envelope parent is required even when core state is equal.
        writer = GitCheckpointStore(cls.bare, branch=acceptance.BRANCH)
        try:
            with writer._repository() as repo:
                head = result["commit"]
                writer._fetch(repo, head)
                writer._checked(repo, "read-tree", head)
                envelope = {"version": 1, "previous_commit": head,
                            "state_sha256": result["core_state_sha256"], "state": cls.state}
                for name, raw in ((acceptance.MANIFEST_PATH, cls.manifest_raw), (CHECKPOINT_PATH, _canonical(envelope))):
                    object_id = writer._checked(repo, "hash-object", "-w", "--stdin", data=raw).decode().strip()
                    writer._checked(repo, "update-index", "--add", "--cacheinfo", "100644", object_id, name)
                tree = writer._checked(repo, "write-tree").decode().strip()
                cls.commit = writer._checked(repo, "commit-tree", tree, "-p", head,
                                              data=b"Synthetic private validation metadata\n").decode().strip()
                if writer._push(repo, cls.commit).returncode:
                    raise AssertionError("Synthetic local fixture push failed")
        finally:
            writer.close()

    def setUp(self):
        self.output = Path(tempfile.mkdtemp(prefix="restore-", dir=self.root))
        self.store = SyntheticReadStore(self.bare)
        self.addCleanup(self.store.close)

    def run_check(self, **changes):
        values = dict(core=self.store, directory=self.output, expected_commit=self.commit,
                      source_manifest_sha256=self.source_hash, expected_account_sha256=self.account_hash)
        values.update(changes)
        return acceptance.run_acceptance(**values)

    def test_fresh_cold_restore_verifies_all_private_evidence_without_broker_or_remote_write(self):
        with patch("socket.create_connection", side_effect=AssertionError("No real network")), \
                patch("zerotrader.broker.PaperBroker", side_effect=AssertionError("No broker")), \
                patch.object(self.store, "_push", side_effect=AssertionError("No remote write")):
            result = self.run_check()
        self.assertEqual(result["status"], "verified")
        self.assertEqual(result["fill_counts"], {"raw_fill_event_rows": 2, "raw_fill_versions": 2,
                                               "distinct_fill_activity_ids": 1, "correction_versions": 1})
        self.assertTrue(result["original_core_and_order_lineage_match"])
        self.assertFalse(result["source_database_byte_hash_checked"])
        self.assertTrue(result["full_archive_logical_hash_checked"])
        self.assertFalse(result["production_migration_verified"])
        self.assertFalse(result["remote_write_test_performed"])
        self.assertFalse((self.output / "cloud-cutover.json").exists())
        for private in (self.state["account_id"], self.account_hash, str(self.root),
                        "synthetic-persisted-original-intent", "synthetic-activity", "synthetic-order"):
            self.assertNotIn(private, json.dumps(result))

    def test_wrong_head_account_or_source_hash_stops_before_archive_restore(self):
        for change in ({"expected_commit": "0" * 40}, {"expected_account_sha256": "0" * 64},
                       {"source_manifest_sha256": "0" * 64}):
            with self.subTest(change=list(change)), patch.object(acceptance, "GitBundleStore") as factory:
                with self.assertRaises(acceptance.AcceptanceError):
                    self.run_check(bundle_factory=factory, **change)
                factory.assert_not_called()
        self.assertEqual(list(self.output.iterdir()), [])

    def test_manifest_unsafe_paths_false_counts_or_cutover_are_rejected(self):
        for case in ("path", "counts", "cutover"):
            value = json.loads(self.manifest_raw)
            if case == "path":
                value["files"][0]["path"] = "../outside.json"
            elif case == "counts":
                value["archive"]["raw_fill_versions"] = True
            else:
                value["cutover_proof"] = True
            raw = json.dumps(value).encode()
            with self.subTest(case=case), self.assertRaises(acceptance.AcceptanceError):
                acceptance._manifest(raw, hashlib.sha256(raw).hexdigest())

    def test_excluded_underscore_metadata_is_valid_without_authorizing_file_reads(self):
        value = json.loads(self.manifest_raw)
        value["auxiliary_excluded"] = [{
            "path": "reviews/peer-code-synthetic/vendor/synthetic/__init__.py",
            "reason": "historical_peer_source_attachment_retained_locally"}, {
            "path": "reviews/peer-code-synthetic/vendor/synthetic/_helper.py",
            "reason": "historical_peer_source_attachment_retained_locally"}]
        raw = json.dumps(value).encode()
        parsed = acceptance._manifest(raw, hashlib.sha256(raw).hexdigest())
        self.assertEqual(parsed["auxiliary_excluded"], value["auxiliary_excluded"])
        for item in parsed["auxiliary_excluded"]:
            self.assertFalse(acceptance.cloud_auxiliary.allowed_path(item["path"]))
        for forbidden in ("reviews/peer-code-synthetic/../escape.py", "reviews/_helper.py"):
            value["auxiliary_excluded"][0]["path"] = forbidden
            raw = json.dumps(value).encode()
            with self.assertRaises(acceptance.AcceptanceError):
                acceptance._manifest(raw, hashlib.sha256(raw).hexdigest())

    def test_unexpected_post_snapshot_trading_record_is_rejected(self):
        copy = self.output / "copy"
        shutil.copytree(self.working, copy)
        archive = ResearchArchive(copy)
        run = archive.start_run("unexpected_synthetic_cycle", {}, {})
        archive.event(run, "unexpected_synthetic_event", {})
        archive.finish_run(run, "completed")
        with self.assertRaises(acceptance.AcceptanceError):
            acceptance.verify_restored_evidence(archive.path, self.output, self.state, self.manifest, self.source_hash)

    def test_all_write_methods_and_git_mutations_are_blocked(self):
        for operation in (lambda: self.store.bootstrap({}), lambda: self.store.commit({}, expected_head=self.commit),
                          lambda: self.store._write({}, None), lambda: self.store._push("unused", self.commit),
                          lambda: self.store._git("unused", "push", "unused"),
                          lambda: self.store._git("unused", "commit-tree", "unused")):
            with self.assertRaises(acceptance.AcceptanceError):
                operation()
        self.assertEqual(self.store._ref, "refs/heads/offline-validation")

    def test_cli_rejects_unreviewed_context_before_secret_or_store_access(self):
        factory = Mock(side_effect=AssertionError("Store must not be constructed"))
        output = io.StringIO()
        with redirect_stdout(output):
            code = acceptance.main([], environ={"GITHUB_ACTIONS": "false"}, core_factory=factory)
        factory.assert_not_called()
        self.assertEqual(code, 1)
        self.assertFalse(json.loads(output.getvalue())["trading_enabled"])

    def test_cli_fixed_failure_does_not_echo_private_error_or_arguments(self):
        env = {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": acceptance.CODE_REPOSITORY,
            "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch",
            "RUNNER_TEMP": str(self.root), "GITHUB_WORKSPACE": str(self.root / "public-workspace"),
            "ZOH_BASELINE_COMMIT": self.commit, "ZOH_SOURCE_MANIFEST_SHA256": self.source_hash,
            "ZOH_EXPECTED_ACCOUNT_SHA256": "\ufeff" + self.account_hash + "\r\n",
            "ZOH_STATE_GITHUB_TOKEN": "synthetic-state-token-not-real"}
        output = io.StringIO()
        with redirect_stdout(output):
            code = acceptance.main([], environ=env, core_factory=Mock(side_effect=RuntimeError("private-error-body")))
        self.assertEqual(code, 1)
        for private in ("private-error-body", str(self.root), self.account_hash, env["ZOH_STATE_GITHUB_TOKEN"]):
            self.assertNotIn(private, output.getvalue())

    def test_manual_workflow_exposes_only_state_secret_and_account_hash(self):
        root = Path(__file__).resolve().parents[1]
        workflow = root / ".github/workflows/private-baseline-acceptance.yml"
        if not workflow.is_file():
            workflow = root / "cloud-main/private-baseline-acceptance.yml"
        raw = workflow.read_text()
        self.assertIn("workflow_dispatch:", raw)
        self.assertNotIn("schedule:", raw)
        self.assertNotIn("upload-artifact", raw)
        self.assertNotIn("actions/cache", raw)
        self.assertNotIn("ZOH_MAIN_PAPER", raw)
        self.assertIn("ubuntu-24.04", raw)
        self.assertIn("persist-credentials: false", raw)


if __name__ == "__main__":
    unittest.main()
