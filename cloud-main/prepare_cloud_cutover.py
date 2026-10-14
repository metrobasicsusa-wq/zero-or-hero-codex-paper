"""Prepare a LOCAL production-branch baseline from already verified host evidence.

This tool does not stop a worker, disable a task, read credentials, contact a
broker/GitHub, or enable trading. The caller MUST obtain the evidence file and
its expected digest through the verified current-user native helper and direct
host-side readback. Matching hashes bind those reviewed bytes; they are NOT
cryptographic attestation of Windows observations or proof of a continuing
execution lease. A final native stop/task/child check is required before enable.

Readonly snapshot helpers retain their original contract. Only a NEW private
working copy receives the supplied evidence, cloud-cutover.json, and an appended
archive event. The resulting bare Git core-checkpoints branch stays local.
Incomplete private outputs are retained on failure. Their mere existence is
not success: only the final verified-cutover-baseline.json receipt confirms the
fresh restore passed, and even that does not authorize trading or publication.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import importlib.util
import json
from pathlib import Path
import re
import sys
import time

_SOURCE_ROOT = Path(__file__).resolve().parents[1]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from zerotrader import cloud_auxiliary
from zerotrader.archive import ResearchArchive
from zerotrader.cloud_bundle import GitBundleStore
from zerotrader.cloud_checkpoint import GitCheckpointStore, _canonical
from zerotrader.cloud_runner import CODE_REPOSITORY, STATE_REPOSITORY, account_digest, validate_cutover

_spec = importlib.util.spec_from_file_location(
    "zoh_cutover_offline_helpers", Path(__file__).with_name("bootstrap_private_state.py"))
_offline = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_offline)
_snapshot = _offline._snapshot

BRANCH = "core-checkpoints"
MAX_EVIDENCE_AGE_SECONDS = 300
MAX_SNAPSHOT_AGE_SECONDS = 900
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40}\Z")
_EVIDENCE_FIELDS = frozenset({
    "version", "kind", "runtime_binding_sha256", "account_sha256",
    "code_repository", "code_commit", "snapshot_manifest_sha256", "core_state_sha256",
    "task_registered", "task_identity_verified", "task_enabled", "stop_requested",
    "stop_flag_present", "verified_process_count", "active_child_process_count",
    "stopped_at", "disabled_at", "observed_at",
})


class CutoverPreparationError(RuntimeError):
    """Fixed diagnostics without private paths, identifiers, or evidence values."""


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _utc(value):
    try:
        if not isinstance(value, str):
            raise ValueError
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None or result.utcoffset() != timedelta(0):
            raise ValueError
        return result
    except (ValueError, TypeError):
        raise CutoverPreparationError("utc_evidence_timestamp_required") from None


def _evidence(path, expected_digest, manifest, manifest_digest, state,
              expected_runtime_binding, expected_account_digest, code_commit, now):
    if (any(not isinstance(value, str) or not _HASH.fullmatch(value) for value in
            (expected_digest, expected_runtime_binding, expected_account_digest))
            or not isinstance(code_commit, str) or not _OID.fullmatch(code_commit)):
        raise CutoverPreparationError("reviewed_bindings_required")
    if _snapshot._regular(path).st_size > 64 * 1024:
        raise CutoverPreparationError("native_evidence_capacity_exceeded")
    raw = path.read_bytes()
    if not hmac.compare_digest(_sha(raw), expected_digest):
        raise CutoverPreparationError("native_evidence_digest_mismatch")
    try:
        value = json.loads(raw)
        if (not isinstance(value, dict) or set(value) != _EVIDENCE_FIELDS
                or type(value["version"]) is not int or value["version"] != 1
                or value["kind"] != "native_cloud_cutover_evidence"
                or value["code_repository"] != CODE_REPOSITORY
                or value["code_commit"] != code_commit
                or value["runtime_binding_sha256"] != expected_runtime_binding
                or value["account_sha256"] != expected_account_digest
                or value["snapshot_manifest_sha256"] != manifest_digest
                or value["core_state_sha256"] != manifest["core_state_sha256"]
                or not hmac.compare_digest(account_digest(state), expected_account_digest)):
            raise CutoverPreparationError("native_evidence_binding_mismatch")
        if (any(value[name] is not True for name in
                ("task_registered", "task_identity_verified", "stop_requested", "stop_flag_present"))
                or value["task_enabled"] is not False
                or any(type(value[name]) is not int or value[name] != 0 for name in
                       ("verified_process_count", "active_child_process_count"))):
            raise CutoverPreparationError("exclusive_stopped_host_not_confirmed")
        stopped, disabled, observed = (_utc(value[name]) for name in
                                      ("stopped_at", "disabled_at", "observed_at"))
        started, finished, verified = (_utc(manifest[name]) for name in
                                      ("capture_started_at", "capture_finished_at", "verified_at"))
        if (not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0)
                or not max(stopped, disabled) <= started <= finished <= verified <= observed <= now):
            raise CutoverPreparationError("native_snapshot_time_order_invalid")
        if ((now - observed).total_seconds() > MAX_EVIDENCE_AGE_SECONDS
                or (now - finished).total_seconds() > MAX_SNAPSHOT_AGE_SECONDS):
            raise CutoverPreparationError("native_or_snapshot_evidence_stale")
        return value, raw
    except CutoverPreparationError:
        raise
    except Exception:
        raise CutoverPreparationError("native_evidence_invalid") from None


def prepare_cloud_cutover(snapshot, destination, manifest_sha256, native_evidence,
                          native_evidence_sha256, runtime_binding_sha256,
                          account_sha256, code_commit, *, now=None):
    """Produce a verified local bundle, never remote publication or activation."""
    began = time.monotonic()
    now = now or datetime.now(timezone.utc)
    try:
        snapshot, destination, native_evidence = map(Path, (snapshot, destination, native_evidence))
        if not all(path.is_absolute() for path in (snapshot, destination, native_evidence)):
            raise CutoverPreparationError("absolute_private_paths_required")
        for path in (snapshot, destination, native_evidence):
            _snapshot._no_links(path)
        snapshot, destination, native_evidence = (path.resolve() for path in
                                                 (snapshot, destination, native_evidence))
        if (not snapshot.is_dir() or not destination.parent.is_dir() or destination.exists()
                or any(path.is_relative_to(_SOURCE_ROOT) for path in (snapshot, destination, native_evidence))
                or snapshot.is_relative_to(destination) or destination.is_relative_to(snapshot)
                or native_evidence.is_relative_to(destination)):
            raise CutoverPreparationError("new_separate_private_destination_required")
        manifest, source_manifest, state, config, source_stats = _offline._verified_source(snapshot, manifest_sha256)
        evidence, evidence_raw = _evidence(native_evidence, native_evidence_sha256, manifest,
            manifest_sha256, state, runtime_binding_sha256, account_sha256, code_commit, now)
        proof = {"version": 1, "authority": "github-actions", "code_repository": CODE_REPOSITORY,
                 "state_repository": STATE_REPOSITORY, "branch": "main", "account_sha256": account_sha256,
                 "local_worker_stopped": True, "local_logon_task_disabled": True,
                 "stopped_at": evidence["stopped_at"], "source_commit": code_commit,
                 "evidence_cutoff": source_stats["evidence_cutoff_sequence"]["events"]}
        validate_cutover(proof, state, account_sha256, now=now)
        proof_raw = _canonical(proof)
        _offline._new_directory(destination)
        working = destination / "working-runtime"
        _offline._new_directory(working)
        for item in manifest["files"]:
            _offline._copy_verified(snapshot / item["path"], working / item["path"], item)
        _snapshot._new_file(destination / "source-manifest.json", source_manifest)
        evidence_name = "reviews/cloud-cutover-native-evidence-" + native_evidence_sha256 + ".json"
        _snapshot._new_file(working / evidence_name, evidence_raw)
        _snapshot._new_file(working / "cloud-cutover.json", proof_raw)
        archive = ResearchArchive(working)
        try:
            run_id = archive.start_run("cloud_cutover_preparation", config, state["strategies"])
            archive.event(run_id, "cloud_cutover_preparation", {
                "source_manifest_sha256": manifest_sha256, "native_evidence_sha256": native_evidence_sha256,
                "proof_sha256": _sha(proof_raw), "source_core_sha256": manifest["core_state_sha256"],
                "source_evidence_cutoff": source_stats["evidence_cutoff_sequence"],
                "source_table_counts": source_stats["table_counts"], "code_commit": code_commit,
                "local_preparation_only": True, "trading_enabled": False,
                "requires_final_native_recheck": True,
                "evidence_origin": "caller_verified_native_readback_not_cryptographic_attestation"})
            cloud_auxiliary.capture_auxiliary(archive, run_id, working)
            if manifest["auxiliary_excluded"]:
                with archive._connect() as db:
                    auxiliary = cloud_auxiliary._read_manifest(db)
                auxiliary["excluded"] = sorted({item["path"]: item for item in
                    auxiliary["excluded"] + manifest["auxiliary_excluded"]}.values(), key=lambda item: item["path"])
                archive.event(run_id, cloud_auxiliary.MANIFEST_KIND, auxiliary)
            archive.finish_run(run_id, "completed", {"local_preparation_only": True, "trading_enabled": False})
        finally:
            archive.close()
        prepared_stats = _offline._stats(archive.path)
        if (any(prepared_stats[key] != source_stats[key] for key in _offline._FILL_FIELDS)
                or any(prepared_stats["table_counts"][key] != source_stats["table_counts"][key]
                       for key in ("samples", "datasets"))
                or prepared_stats["table_counts"]["runs"] != source_stats["table_counts"]["runs"] + 1
                or (working / "state.json").read_bytes() != (snapshot / "state.json").read_bytes()):
            raise CutoverPreparationError("preparation_changed_trading_evidence")
        bare = destination / "baseline.git"
        _offline._new_directory(bare)
        core = GitCheckpointStore(bare, branch=BRANCH, timeout_seconds=180)
        try:
            core._checked(bare, "init", "--bare", "--quiet")
            with GitBundleStore(core) as bundle:
                receipt = bundle.bootstrap(state, archive.path)
        finally:
            core.close()
        restored = destination / "verified-restore"
        _offline._new_directory(restored)
        (restored / "archive").mkdir(mode=0o700)
        restored_archive = restored / "archive/research.sqlite3"
        fresh_core = GitCheckpointStore(bare, branch=BRANCH, timeout_seconds=180)
        try:
            with GitBundleStore(fresh_core) as bundle:
                loaded = bundle.restore(restored_archive)
        finally:
            fresh_core.close()
        if loaded.receipt != receipt or _canonical(loaded.state) != _canonical(state):
            raise CutoverPreparationError("fresh_restore_core_mismatch")
        restored_stats = _offline._stats(restored_archive)
        if restored_stats != prepared_stats or loaded.counts != prepared_stats["table_counts"]:
            raise CutoverPreparationError("fresh_restore_archive_mismatch")
        auxiliary = cloud_auxiliary.restore_auxiliary(restored_archive, restored)
        for item in manifest["files"]:
            if item["path"] not in ("state.json", "archive/research.sqlite3"):
                copied = restored / item["path"]
                if copied.stat().st_size != item["bytes"] or _snapshot._file_hash(copied) != item["sha256"]:
                    raise CutoverPreparationError("fresh_restore_auxiliary_mismatch")
        if ((restored / "cloud-cutover.json").read_bytes() != proof_raw
                or (restored / evidence_name).read_bytes() != evidence_raw):
            raise CutoverPreparationError("fresh_restore_cutover_evidence_mismatch")
        validate_cutover(json.loads((restored / "cloud-cutover.json").read_bytes()), loaded.state,
                         account_sha256, now=now)
        _snapshot._new_file(restored / "state.json", _canonical(loaded.state))
        _offline._verified_source(snapshot, manifest_sha256)
        if native_evidence.read_bytes() != evidence_raw:
            raise CutoverPreparationError("native_evidence_changed_during_preparation")
        result = {"status": "verified_local_cutover_baseline", "branch": BRANCH,
                  "local_preparation_only": True, "trading_enabled": False, "remote_published": False,
                  "requires_final_native_recheck": True,
                  "source_manifest_sha256": manifest_sha256, "native_evidence_sha256": native_evidence_sha256,
                  "proof_sha256": _sha(proof_raw), "code_commit": code_commit,
                  "core_state_sha256": receipt.state_sha256, "commit": receipt.commit,
                  "archive_manifest_sha256": receipt.archive_manifest_sha256,
                  "archive_snapshot_sha256": receipt.archive_snapshot_sha256,
                  "archive_index_sha256": receipt.archive_index_sha256, "generation": receipt.generation,
                  "source_table_counts": source_stats["table_counts"], "prepared_table_counts": prepared_stats["table_counts"],
                  "restored_table_counts": restored_stats["table_counts"],
                  "fill_counts": {key: restored_stats[key] for key in _offline._FILL_FIELDS},
                  "source_evidence_cutoff": source_stats["evidence_cutoff_sequence"],
                  "restored_evidence_cutoff": restored_stats["evidence_cutoff_sequence"],
                  "auxiliary_file_count": auxiliary["files"], "auxiliary_manifest_sha256": auxiliary["manifest_sha256"],
                  "excluded_historical_attachment_count": len(auxiliary["excluded"]),
                  "verified_at": datetime.now(timezone.utc).isoformat(),
                  "elapsed_seconds": round(time.monotonic() - began, 3)}
        _snapshot._new_file(destination / "verified-cutover-baseline.json", _canonical(result))
        return result
    except CutoverPreparationError:
        raise
    except Exception:
        raise CutoverPreparationError("local_cutover_preparation_failed") from None


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    names = {"--snapshot", "--destination", "--manifest-sha256", "--native-evidence", "--native-evidence-sha256",
             "--runtime-binding-sha256", "--account-sha256", "--code-commit"}
    try:
        if len(args) != 16 or set(args[::2]) != names:
            raise CutoverPreparationError("invalid_arguments")
        options = dict(zip(args[::2], args[1::2]))
        result = prepare_cloud_cutover(*(options[name] for name in
            ("--snapshot", "--destination", "--manifest-sha256", "--native-evidence", "--native-evidence-sha256",
             "--runtime-binding-sha256", "--account-sha256", "--code-commit")))
    except CutoverPreparationError as error:
        result = {"status": "failed", "error_category": str(error), "trading_enabled": False,
                  "local_preparation_only": True, "remote_published": False}
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
