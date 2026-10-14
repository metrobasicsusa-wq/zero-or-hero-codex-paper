"""Manual, read-only cold recovery acceptance for the PRIVATE offline baseline.

This entry point cannot trade, bootstrap, push, mint cutover proof or enable a
workflow. Its only remote is the fixed private state repository and its only
branch is offline-validation. All restored evidence stays in runner temporary
storage, never the public workspace or Actions artifacts. Public output is a
fixed set of counts, hashes, timings and explicit verification limitations.
"""
from contextlib import closing
import hashlib
import hmac
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from zerotrader import cloud_auxiliary
from zerotrader.archive import ResearchArchive
from zerotrader.cloud_bundle import GitBundleStore
from zerotrader.cloud_checkpoint import CheckpointError, _canonical, _check_state
from zerotrader.cloud_github import PrivateGithubCheckpointStore, normalize_secret_text

_spec = importlib.util.spec_from_file_location("zoh_readonly_snapshot_checks",
                                             Path(__file__).with_name("prepare_migration_snapshot.py"))
_snapshot = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_snapshot)

CODE_REPOSITORY = "metrobasicsusa-wq/zero-or-hero-codex-paper"
BRANCH = "offline-validation"
MANIFEST_PATH = "source-manifest.json"
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_FILL_FIELDS = ("raw_fill_event_rows", "raw_fill_versions", "distinct_fill_activity_ids", "correction_versions")
_TABLES = {"blobs", "runs", "events", "samples", "datasets"}
_SEQUENCES = {"events", "samples", "datasets"}
_MANIFEST_FIELDS = {"format_version", "snapshot_kind", "cutover_proof", "trading_enabled",
    "fresh_cutover_snapshot_required", "capture_started_at", "capture_finished_at", "verified_at",
    "core_state_sha256", "archive", "files", "auxiliary_excluded", "auxiliary_atomic_with_archive",
    "fill_count_semantics"}


class AcceptanceError(RuntimeError):
    """Fixed diagnostic, never raw exception text or private content."""


class ReadOnlyOfflineStore(PrivateGithubCheckpointStore):
    def __init__(self, *, credential_loader=None):
        super().__init__(branch=BRANCH, credential_loader=credential_loader, timeout_seconds=180)

    def _git(self, directory, *args, data=None):
        if not args or args[0] not in {"init", "ls-remote", "fetch", "rev-parse", "show",
                                       "rev-list", "cat-file", "ls-tree", "merge-base"}:
            raise AcceptanceError("remote_writes_forbidden")
        return super()._git(directory, *args, data=data)

    def _forbidden(self, *args, **kwargs):
        raise AcceptanceError("remote_writes_forbidden")

    bootstrap = commit = _write = _push = _forbidden

    def read_source_manifest(self, receipt):
        """One fixed, bounded regular blob, at the still-current pinned head."""
        self.confirm(receipt)
        with self._repository() as directory:
            if self._head(directory) != receipt.commit:
                raise AcceptanceError("baseline_head_changed")
            self._fetch(directory, receipt.commit)
            raw = self._checked(directory, "ls-tree", "-z", receipt.commit, "--", MANIFEST_PATH)
            pieces = raw.split(b"\0")
            if len(pieces) != 2 or pieces[1]:
                raise AcceptanceError("source_manifest_missing_or_invalid")
            metadata, name = pieces[0].split(b"\t", 1)
            mode, kind, object_id = metadata.decode("ascii").split()
            if mode != "100644" or kind != "blob" or name != MANIFEST_PATH.encode("ascii"):
                raise AcceptanceError("source_manifest_missing_or_invalid")
            spec = receipt.commit + ":" + MANIFEST_PATH
            size = int(self._checked(directory, "cat-file", "-s", spec).strip())
            if not 0 < size <= MAX_MANIFEST_BYTES:
                raise AcceptanceError("source_manifest_missing_or_invalid")
            value = self._checked(directory, "cat-file", "blob", spec)
            if len(value) != size or self._head(directory) != receipt.commit:
                raise AcceptanceError("baseline_head_changed")
        return value


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _counts(value, keys):
    return (isinstance(value, dict) and set(value) == keys
            and all(type(count) is int and count >= 0 for count in value.values()))


def _manifest(raw, expected_hash):
    if not isinstance(raw, bytes) or len(raw) > MAX_MANIFEST_BYTES or _sha(raw) != expected_hash:
        raise AcceptanceError("source_manifest_hash_mismatch")
    try:
        value = json.loads(raw)
        if (not isinstance(value, dict) or set(value) != _MANIFEST_FIELDS
                or type(value["format_version"]) is not int or value["format_version"] != 1
                or value["snapshot_kind"] != "readonly_validation"
                or value["cutover_proof"] is not False or value["trading_enabled"] is not False
                or value["fresh_cutover_snapshot_required"] is not True
                or value["auxiliary_atomic_with_archive"] is not False
                or not _HASH.fullmatch(value["core_state_sha256"])
                or not isinstance(value["files"], list)
                or len(value["files"]) > cloud_auxiliary.MAX_FILES + 2):
            raise ValueError
        stats = value["archive"]
        if (not isinstance(stats, dict) or set(stats) != {"table_counts", "evidence_cutoff_sequence", *_FILL_FIELDS}
                or not _counts(stats["table_counts"], _TABLES)
                or not _counts(stats["evidence_cutoff_sequence"], _SEQUENCES)
                or any(type(stats[key]) is not int or stats[key] < 0 for key in _FILL_FIELDS)):
            raise ValueError
        names = set()
        for item in value["files"]:
            if (not isinstance(item, dict) or set(item) != {"path", "sha256", "bytes"}
                    or not isinstance(item["path"], str) or item["path"].casefold() in names
                    or item["path"] == "cloud-cutover.json"
                    or (item["path"] not in {"state.json", "archive/research.sqlite3"}
                        and not cloud_auxiliary.allowed_path(item["path"]))
                    or not _HASH.fullmatch(item["sha256"])
                    or type(item["bytes"]) is not int or item["bytes"] < 0):
                raise ValueError
            names.add(item["path"].casefold())
        if not {"state.json", "archive/research.sqlite3"}.issubset(names):
            raise ValueError
        if not isinstance(value["auxiliary_excluded"], list):
            raise ValueError
        for item in value["auxiliary_excluded"]:
            if (not isinstance(item, dict) or set(item) != {"path", "reason"}
                    or not cloud_auxiliary._valid_path_syntax(item["path"])
                    or not cloud_auxiliary._excluded_reason(item["path"])
                    or cloud_auxiliary._excluded_reason(item["path"]) != item["reason"]):
                raise ValueError
        return value
    except Exception:
        raise AcceptanceError("source_manifest_invalid") from None


def _binding(state, expected_account_hash, expected_state_hash=None):
    try:
        _check_state(state)
        digest = _sha(state["account_id"].encode("utf-8"))
        if not hmac.compare_digest(digest, expected_account_hash):
            raise ValueError
        if expected_state_hash is not None and _sha(_canonical(state)) != expected_state_hash:
            raise ValueError
    except Exception:
        raise AcceptanceError("account_or_core_binding_mismatch") from None


def verify_restored_evidence(archive_path, directory, state, manifest, source_hash):
    """Verify source prefix, complete FILL versions and exact restored aux bytes.

    The restored SQLite has migration/aux records added, and physical SQLite
    pages can differ after logical rebuild. Its bytes cannot equal the original
    snapshot DB hash. Bundle verification instead validates every archived blob,
    source closure and the complete reconstructed logical snapshot hash.
    """
    _binding(state, _sha(state["account_id"].encode()), manifest["core_state_sha256"])
    source = manifest["archive"]
    with closing(sqlite3.connect(archive_path.as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
        db.execute("PRAGMA query_only=ON")
        stats = _snapshot._archive_stats(db, validate_fills=True)
        cutoff = source["evidence_cutoff_sequence"]
        for name in _SEQUENCES:
            before = db.execute("SELECT COUNT(*) FROM " + name + " WHERE sequence<=?", (cutoff[name],)).fetchone()[0]
            if before != source["table_counts"][name]:
                raise AcceptanceError("source_evidence_count_mismatch")
        if (any(stats[key] != source[key] for key in _FILL_FIELDS)
                or any(stats["table_counts"][name] != source["table_counts"][name] for name in ("samples", "datasets"))
                or any(stats["evidence_cutoff_sequence"][name] != cutoff[name] for name in ("samples", "datasets"))
                or stats["table_counts"]["runs"] != source["table_counts"]["runs"] + 1
                or stats["table_counts"]["blobs"] < source["table_counts"]["blobs"]):
            raise AcceptanceError("source_evidence_count_mismatch")
        additions = db.execute("SELECT run_id,kind,payload_hash FROM events WHERE sequence>? ORDER BY sequence",
                               (cutoff["events"],)).fetchall()
        migration = [row for row in additions if row["kind"] == "cloud_migration_snapshot"]
        if len(migration) != 1:
            raise AcceptanceError("migration_evidence_invalid")
        run_id = migration[0]["run_id"]
        if any(row["run_id"] != run_id or row["kind"] not in {
            "run_started", "run_finished", "cloud_migration_snapshot",
            cloud_auxiliary.FILE_KIND, cloud_auxiliary.MANIFEST_KIND} for row in additions):
            raise AcceptanceError("unexpected_post_snapshot_activity")
        started = [row for row in additions if row["kind"] == "run_started"]
        finished = [row for row in additions if row["kind"] == "run_finished"]
        if len(started) != 1 or len(finished) != 1:
            raise AcceptanceError("migration_evidence_invalid")
        if ResearchArchive._get(db, started[0]["payload_hash"]) != {"action": "cloud_migration_snapshot"}:
            raise AcceptanceError("migration_evidence_invalid")
        event = ResearchArchive._get(db, migration[0]["payload_hash"])
        expected = {"snapshot_kind": "readonly_validation", "trading_enabled": False, "cutover_proof": False,
            "source_manifest_sha256": source_hash, "source_core_sha256": manifest["core_state_sha256"],
            "source_evidence_cutoff": cutoff, "source_table_counts": source["table_counts"],
            "source_fill_counts": {key: source[key] for key in _FILL_FIELDS},
            "auxiliary_excluded": manifest["auxiliary_excluded"]}
        if event != expected:
            raise AcceptanceError("migration_evidence_invalid")
        run = db.execute("SELECT action,status,snapshot_hash,error_json FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if (run is None or tuple(run[:2]) != ("cloud_migration_snapshot", "completed") or run["error_json"] is not None
                or ResearchArchive._get(db, run["snapshot_hash"]) != {"offline_validation": True, "trading_enabled": False}
                or ResearchArchive._get(db, finished[0]["payload_hash"]) != {
                    "status": "completed", "snapshot_hash": run["snapshot_hash"], "error": None}):
            raise AcceptanceError("migration_evidence_invalid")
    auxiliary = cloud_auxiliary.restore_auxiliary(archive_path, directory)
    expected_files = [item for item in manifest["files"] if item["path"] not in {"state.json", "archive/research.sqlite3"}]
    if auxiliary["files"] != len(expected_files) or auxiliary["excluded"] != sorted(manifest["auxiliary_excluded"], key=lambda item: item["path"]):
        raise AcceptanceError("auxiliary_manifest_mismatch")
    for item in expected_files:
        target = directory / item["path"]
        if (_snapshot._regular(target).st_size != item["bytes"] or _snapshot._file_hash(target) != item["sha256"]):
            raise AcceptanceError("auxiliary_file_hash_mismatch")
    if (directory / "cloud-cutover.json").exists():
        raise AcceptanceError("offline_baseline_contains_cutover_proof")
    return stats, auxiliary


def run_acceptance(core, directory, expected_commit, source_manifest_sha256, expected_account_sha256,
                   *, bundle_factory=GitBundleStore, clock=time.monotonic):
    started = clock()
    if (not isinstance(expected_commit, str) or not _COMMIT.fullmatch(expected_commit)
            or not isinstance(source_manifest_sha256, str) or not _HASH.fullmatch(source_manifest_sha256)
            or not isinstance(expected_account_sha256, str) or not _HASH.fullmatch(expected_account_sha256)):
        raise AcceptanceError("explicit_reviewed_bindings_required")
    directory = Path(directory)
    if not directory.is_absolute() or not directory.is_dir() or directory.is_symlink() or any(directory.iterdir()):
        raise AcceptanceError("empty_private_temporary_directory_required")
    core.verify_private_repository()
    initial = core.load()
    if initial.receipt.commit != expected_commit:
        raise AcceptanceError("baseline_commit_mismatch")
    _binding(initial.state, expected_account_sha256)
    manifest = _manifest(core.read_source_manifest(initial.receipt), source_manifest_sha256)
    _binding(initial.state, expected_account_sha256, manifest["core_state_sha256"])
    metadata_done = clock()
    archive_path = directory / "archive/research.sqlite3"
    archive_path.parent.mkdir(mode=0o700)
    with bundle_factory(core) as bundle:
        loaded = bundle.restore(archive_path)
    restored = clock()
    if loaded.receipt.commit != expected_commit or loaded.receipt.generation != 0:
        raise AcceptanceError("offline_baseline_generation_mismatch")
    _binding(loaded.state, expected_account_sha256, manifest["core_state_sha256"])
    stats, auxiliary = verify_restored_evidence(archive_path, directory, loaded.state, manifest, source_manifest_sha256)
    if stats["table_counts"] != loaded.counts:
        raise AcceptanceError("archive_bundle_count_mismatch")
    core.confirm(loaded.receipt)
    return {"status": "verified", "mode": "readonly_offline_baseline", "branch": BRANCH,
        "trading_enabled": False, "cutover_proof": False, "remote_write_test_performed": False,
        "production_migration_verified": False, "account_binding_match": True,
        "original_core_and_order_lineage_match": True, "source_manifest_sha256": source_manifest_sha256,
        "commit": loaded.receipt.commit, "core_state_sha256": loaded.receipt.state_sha256,
        "archive_manifest_sha256": loaded.receipt.archive_manifest_sha256,
        "archive_snapshot_sha256": loaded.receipt.archive_snapshot_sha256,
        "archive_index_sha256": loaded.receipt.archive_index_sha256, "generation": loaded.receipt.generation,
        "source_table_counts": manifest["archive"]["table_counts"], "restored_table_counts": stats["table_counts"],
        "source_evidence_cutoff": manifest["archive"]["evidence_cutoff_sequence"],
        "restored_evidence_cutoff": stats["evidence_cutoff_sequence"],
        "fill_counts": {key: stats[key] for key in _FILL_FIELDS},
        "auxiliary_file_count": auxiliary["files"], "auxiliary_total_bytes": auxiliary["total_bytes"],
        "auxiliary_manifest_sha256": auxiliary["manifest_sha256"],
        "excluded_historical_attachment_count": len(auxiliary["excluded"]),
        "source_database_byte_hash_checked": False, "full_archive_logical_hash_checked": True,
        "original_auxiliary_atomic_with_archive": False,
        "metadata_seconds": round(metadata_done - started, 3), "cold_restore_seconds": round(restored - metadata_done, 3),
        "evidence_verification_seconds": round(clock() - restored, 3), "total_seconds": round(clock() - started, 3)}


def main(argv=None, *, environ=None, core_factory=ReadOnlyOfflineStore, bundle_factory=GitBundleStore):
    try:
        if (sys.argv[1:] if argv is None else argv):
            raise AcceptanceError("arguments_not_supported")
        env = os.environ if environ is None else environ
        if (env.get("GITHUB_ACTIONS") != "true" or env.get("GITHUB_REPOSITORY") != CODE_REPOSITORY
                or env.get("GITHUB_REF") != "refs/heads/main" or env.get("GITHUB_EVENT_NAME") != "workflow_dispatch"):
            raise AcceptanceError("manual_reviewed_workflow_required")
        commit = env.get("ZOH_BASELINE_COMMIT", "")
        source_hash = env.get("ZOH_SOURCE_MANIFEST_SHA256", "")
        account_hash = normalize_secret_text(env.get("ZOH_EXPECTED_ACCOUNT_SHA256", ""))
        if not _COMMIT.fullmatch(commit) or not _HASH.fullmatch(source_hash) or not _HASH.fullmatch(account_hash):
            raise AcceptanceError("explicit_reviewed_bindings_required")
        temporary, workspace = Path(env.get("RUNNER_TEMP", "")), Path(env.get("GITHUB_WORKSPACE", ""))
        if (not temporary.is_absolute() or not temporary.is_dir() or not workspace.is_absolute()
                or temporary.resolve().is_relative_to(workspace.resolve())
                or Path(tempfile.gettempdir()).resolve().is_relative_to(workspace.resolve())):
            raise AcceptanceError("private_runner_temporary_directory_required")
        with tempfile.TemporaryDirectory(prefix="zoh-private-baseline-check-", dir=temporary) as target:
            directory = Path(target)
            directory.chmod(0o700)
            core = core_factory(credential_loader=lambda: normalize_secret_text(env.get("ZOH_STATE_GITHUB_TOKEN", "")))
            try:
                result = run_acceptance(core, directory, commit, source_hash, account_hash, bundle_factory=bundle_factory)
            finally:
                core.close()
        print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
        return 0
    except BaseException:
        print(json.dumps({"status": "failed", "mode": "readonly_offline_baseline", "trading_enabled": False,
                          "cutover_proof": False, "remote_write_test_performed": False,
                          "production_migration_verified": False, "action": "review_private_baseline"}, sort_keys=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
