"""Prepare and verify an OFFLINE private Git baseline from a frozen snapshot.

No broker, credentials, HTTPS transport, cutover creation, or workflow enablement
is implemented. The only Git remote is a NEW local bare repository on the fixed
offline-validation branch. The original snapshot is read-only and unchanged.
This is an acceptance artifact, never authority to start a cloud trader.
"""
from contextlib import closing
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time

_SOURCE_ROOT = Path(__file__).resolve().parents[1]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from zerotrader import cloud_auxiliary
from zerotrader.archive import ResearchArchive
from zerotrader.cloud_bundle import GitBundleStore
from zerotrader.cloud_checkpoint import GitCheckpointStore, _canonical, _check_state

_spec = importlib.util.spec_from_file_location("zoh_offline_snapshot_helpers",
                                             Path(__file__).with_name("prepare_migration_snapshot.py"))
_snapshot = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_snapshot)

BRANCH = "offline-validation"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_MANIFEST_FIELDS = frozenset({
    "format_version", "snapshot_kind", "cutover_proof", "trading_enabled",
    "fresh_cutover_snapshot_required", "capture_started_at", "capture_finished_at",
    "verified_at", "core_state_sha256", "archive", "files", "auxiliary_excluded",
    "auxiliary_atomic_with_archive", "fill_count_semantics",
})
_FILL_FIELDS = ("raw_fill_event_rows", "raw_fill_versions", "distinct_fill_activity_ids", "correction_versions")


class BootstrapError(RuntimeError):
    """Fixed error categories; never include private content or locations."""


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _new_directory(path):
    path.mkdir(mode=0o700, parents=False, exist_ok=False)
    os.chmod(path, 0o700)


def _verified_source(snapshot, expected_manifest_sha256):
    if not isinstance(expected_manifest_sha256, str) or not _HASH.fullmatch(expected_manifest_sha256):
        raise BootstrapError("manifest_digest_required")
    path = snapshot / "manifest.json"
    if _snapshot._regular(path).st_size > 4 * 1024 * 1024:
        raise BootstrapError("manifest_capacity_exceeded")
    raw = path.read_bytes()
    if _sha(raw) != expected_manifest_sha256:
        raise BootstrapError("manifest_digest_mismatch")
    try:
        manifest = json.loads(raw)
        if (not isinstance(manifest, dict) or set(manifest) != _MANIFEST_FIELDS
                or type(manifest["format_version"]) is not int or manifest["format_version"] != 1
                or manifest["snapshot_kind"] != "readonly_validation"
                or manifest["cutover_proof"] is not False or manifest["trading_enabled"] is not False
                or manifest["fresh_cutover_snapshot_required"] is not True
                or manifest["auxiliary_atomic_with_archive"] is not False
                or not isinstance(manifest["files"], list)
                or len(manifest["files"]) > cloud_auxiliary.MAX_FILES + 2
                or not isinstance(manifest["auxiliary_excluded"], list)):
            raise ValueError
        names = set()
        for item in manifest["files"]:
            if (not isinstance(item, dict) or set(item) != {"path", "sha256", "bytes"}
                    or not isinstance(item["path"], str)
                    or item["path"].casefold() in names
                    or (item["path"] not in {"state.json", "archive/research.sqlite3"}
                        and not cloud_auxiliary.allowed_path(item["path"]))
                    or item["path"] == "cloud-cutover.json"
                    or not isinstance(item["sha256"], str) or not _HASH.fullmatch(item["sha256"])
                    or type(item["bytes"]) is not int or item["bytes"] < 0):
                raise ValueError
            names.add(item["path"].casefold())
            source = snapshot / item["path"]
            if (_snapshot._regular(source).st_size != item["bytes"]
                    or _snapshot._file_hash(source) != item["sha256"]):
                raise BootstrapError("snapshot_file_hash_mismatch")
        if not {"state.json", "archive/research.sqlite3"}.issubset(names):
            raise ValueError
        for excluded in manifest["auxiliary_excluded"]:
            if not cloud_auxiliary._valid_exclusion(excluded):
                raise ValueError
        if any((snapshot / name).exists() for name in ("runner.lock", "worker.json", "cloud-cutover.json")):
            raise BootstrapError("frozen_readonly_snapshot_required")
        # prepare_snapshot's read-only validation can leave an EMPTY WAL plus
        # shared-memory bookkeeping next to its closed, fully copied database.
        # Preserve those files untouched. Never ignore a WAL containing bytes:
        # immutable reads would omit its uncheckpointed records.
        wal = snapshot / "archive/research.sqlite3-wal"
        shm = snapshot / "archive/research.sqlite3-shm"
        if wal.exists() and _snapshot._regular(wal).st_size:
            raise BootstrapError("frozen_readonly_snapshot_required")
        if shm.exists():
            _snapshot._regular(shm)
        state = json.loads((snapshot / "state.json").read_text(encoding="utf-8-sig"))
        _check_state(state)
        if _sha(_canonical(state)) != manifest["core_state_sha256"]:
            raise BootstrapError("snapshot_core_hash_mismatch")
        # immutable=1 is appropriate only for this already-hashed frozen copy:
        # it prevents SQLite from creating WAL/SHM files beside the source.
        uri = (snapshot / "archive/research.sqlite3").as_uri() + "?mode=ro&immutable=1"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("PRAGMA trusted_schema=OFF")
            stats = _snapshot._archive_stats(db, validate_fills=True)
            row = db.execute("SELECT config_hash FROM runs ORDER BY started_at DESC,run_id DESC LIMIT 1").fetchone()
            if row is None:
                raise BootstrapError("existing_archive_runs_required")
            config = ResearchArchive._get(db, row[0])
        if stats != manifest["archive"]:
            raise BootstrapError("snapshot_archive_statistics_mismatch")
        return manifest, raw, state, config, stats
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("snapshot_manifest_or_evidence_invalid") from None


def _copy_verified(source, destination, expected):
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    digest, size = hashlib.sha256(), 0
    with source.open("rb") as incoming, destination.open("xb") as outgoing:
        os.chmod(destination, 0o600)
        for block in iter(lambda: incoming.read(1024 * 1024), b""):
            outgoing.write(block)
            digest.update(block)
            size += len(block)
        outgoing.flush()
        os.fsync(outgoing.fileno())
    if size != expected["bytes"] or digest.hexdigest() != expected["sha256"]:
        raise BootstrapError("snapshot_changed_during_copy")


def _stats(path):
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.execute("PRAGMA query_only=ON")
        return _snapshot._archive_stats(db, validate_fills=True)


def prepare_private_baseline(snapshot, destination, expected_manifest_sha256):
    """Create a new private local bare Git baseline and fresh-restore proof.

    No deletion on failure: incomplete private outputs remain for diagnosis.
    A completed receipt is written last. Only counts/hashes are returned.
    """
    began = time.monotonic()
    try:
        snapshot, destination = Path(snapshot), Path(destination)
        if not snapshot.is_absolute() or not destination.is_absolute():
            raise BootstrapError("absolute_private_paths_required")
        _snapshot._no_links(snapshot)
        _snapshot._no_links(destination)
        snapshot, destination = snapshot.resolve(), destination.resolve()
        if (not snapshot.is_dir() or not destination.parent.is_dir() or destination.exists()
                or snapshot.is_relative_to(_SOURCE_ROOT) or destination.is_relative_to(_SOURCE_ROOT)
                or snapshot.is_relative_to(destination) or destination.is_relative_to(snapshot)):
            raise BootstrapError("new_separate_private_destination_required")
        manifest, raw_manifest, state, config, source_stats = _verified_source(snapshot, expected_manifest_sha256)
        _new_directory(destination)
        working = destination / "working-runtime"
        _new_directory(working)
        for item in manifest["files"]:
            _copy_verified(snapshot / item["path"], working / item["path"], item)
        _snapshot._new_file(destination / "source-manifest.json", raw_manifest)
        archive = ResearchArchive(working)
        run_id = archive.start_run("cloud_migration_snapshot", config, state["strategies"])
        archive.event(run_id, "cloud_migration_snapshot", {
            "snapshot_kind": "readonly_validation", "trading_enabled": False, "cutover_proof": False,
            "source_manifest_sha256": expected_manifest_sha256,
            "source_core_sha256": manifest["core_state_sha256"],
            "source_evidence_cutoff": source_stats["evidence_cutoff_sequence"],
            "source_table_counts": source_stats["table_counts"],
            "source_fill_counts": {key: source_stats[key] for key in _FILL_FIELDS},
            "auxiliary_excluded": manifest["auxiliary_excluded"],
        })
        cloud_auxiliary.capture_auxiliary(archive, run_id, working)
        if manifest["auxiliary_excluded"]:
            with archive._connect() as db:
                aux_manifest = cloud_auxiliary._read_manifest(db)
            aux_manifest["excluded"] = sorted({item["path"]: item for item in
                aux_manifest["excluded"] + manifest["auxiliary_excluded"]}.values(), key=lambda item: item["path"])
            archive.event(run_id, cloud_auxiliary.MANIFEST_KIND, aux_manifest)
        archive.finish_run(run_id, "completed", {"offline_validation": True, "trading_enabled": False})
        prepared_stats = _stats(archive.path)
        if (any(prepared_stats[key] != source_stats[key] for key in _FILL_FIELDS)
                or any(prepared_stats["table_counts"][key] != source_stats["table_counts"][key]
                       for key in ("samples", "datasets"))
                or prepared_stats["table_counts"]["runs"] != source_stats["table_counts"]["runs"] + 1):
            raise BootstrapError("preparation_changed_trading_evidence")
        bare = destination / "baseline.git"
        _new_directory(bare)
        core = GitCheckpointStore(bare, branch=BRANCH)
        try:
            core._checked(bare, "init", "--bare", "--quiet")
            with GitBundleStore(core) as bundle:
                receipt = bundle.bootstrap(state, archive.path)
        finally:
            core.close()
        restored = destination / "verified-restore"
        _new_directory(restored)
        (restored / "archive").mkdir(mode=0o700)
        restored_archive = restored / "archive/research.sqlite3"
        fresh_core = GitCheckpointStore(bare, branch=BRANCH)
        try:
            with GitBundleStore(fresh_core) as fresh_bundle:
                loaded = fresh_bundle.restore(restored_archive)
        finally:
            fresh_core.close()
        if loaded.receipt != receipt or _canonical(loaded.state) != _canonical(state):
            raise BootstrapError("fresh_restore_core_or_receipt_mismatch")
        restored_stats = _stats(restored_archive)
        if restored_stats != prepared_stats or loaded.counts != prepared_stats["table_counts"]:
            raise BootstrapError("fresh_restore_archive_statistics_mismatch")
        aux = cloud_auxiliary.restore_auxiliary(restored_archive, restored)
        for item in manifest["files"]:
            if item["path"] in ("state.json", "archive/research.sqlite3"):
                continue
            path = restored / item["path"]
            if path.stat().st_size != item["bytes"] or _snapshot._file_hash(path) != item["sha256"]:
                raise BootstrapError("fresh_restore_auxiliary_mismatch")
        _snapshot._new_file(restored / "state.json", _canonical(loaded.state))
        # Verify all source hashes AGAIN after the complete local Git round trip.
        _verified_source(snapshot, expected_manifest_sha256)
        result = {"status": "verified", "snapshot_kind": "readonly_validation", "trading_enabled": False,
                  "cutover_proof": False, "fresh_cutover_snapshot_required": True, "branch": BRANCH,
                  "source_manifest_sha256": expected_manifest_sha256,
                  "core_state_sha256": receipt.state_sha256, "commit": receipt.commit,
                  "archive_manifest_sha256": receipt.archive_manifest_sha256,
                  "archive_snapshot_sha256": receipt.archive_snapshot_sha256,
                  "archive_index_sha256": receipt.archive_index_sha256, "generation": receipt.generation,
                  "source_table_counts": source_stats["table_counts"], "prepared_table_counts": prepared_stats["table_counts"],
                  "restored_table_counts": restored_stats["table_counts"],
                  "fill_counts": {key: restored_stats[key] for key in _FILL_FIELDS},
                  "source_evidence_cutoff": source_stats["evidence_cutoff_sequence"],
                  "restored_evidence_cutoff": restored_stats["evidence_cutoff_sequence"],
                  "auxiliary_file_count": aux["files"], "auxiliary_total_bytes": aux["total_bytes"],
                  "auxiliary_manifest_sha256": aux["manifest_sha256"],
                  "excluded_historical_attachment_count": len(aux["excluded"]),
                  "elapsed_seconds": round(time.monotonic() - began, 3)}
        _snapshot._new_file(destination / "verified-baseline.json", _canonical(result))
        return result
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("offline_private_baseline_failed") from None


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    try:
        if len(args) != 6 or set(args[::2]) != {"--snapshot", "--destination", "--manifest-sha256"}:
            raise BootstrapError("invalid_arguments")
        options = dict(zip(args[::2], args[1::2]))
        result = prepare_private_baseline(options["--snapshot"], options["--destination"], options["--manifest-sha256"])
    except BootstrapError as error:
        result = {"status": "failed", "error_category": str(error), "snapshot_kind": "readonly_validation",
                  "trading_enabled": False, "cutover_proof": False}
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
