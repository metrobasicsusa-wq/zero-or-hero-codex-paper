"""Prepare a private, offline validation copy; never a trader cutover receipt.

Explicit paths only. The existing runner lock is acquired without waiting. Core
state and SQLite are copied while that lock is held; auxiliary files are copied
only from cloud_auxiliary's reviewed allowlist and checked again for changes.
This relies on cooperating state writers using runner.lock. Auxiliary writers
outside that lock are not claimed to share an atomic database transaction.
No credentials, broker, network, exports or worker logs are accessed. Actual
runtime use must follow the approved native-host and export procedure.
"""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

_SOURCE_ROOT = Path(__file__).resolve().parents[1]
if str(_SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(_SOURCE_ROOT))

from zerotrader import cloud_auxiliary
from zerotrader.archive import ResearchArchive
from zerotrader.archival import fingerprint
from zerotrader.cloud_archive import TABLES, _reviewed_schema, _schema
from zerotrader.cloud_checkpoint import MAX_BYTES, _canonical, _check_state
from zerotrader.storage import process_lock


class SnapshotError(RuntimeError):
    """Fixed errors never include source bytes, private paths or exception text."""


def _now():
    return datetime.now(timezone.utc).isoformat()


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


def _no_links(path):
    for current in (path, *path.parents):
        try:
            info = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise SnapshotError("linked_path_refused")


def _regular(path):
    _no_links(path)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise SnapshotError("nonregular_source_refused")
    return info


def _new_file(path, raw):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("xb") as stream:
        os.chmod(path, 0o600)
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _state_bytes(path):
    if _regular(path).st_size > MAX_BYTES:
        raise SnapshotError("core_state_invalid")
    raw = path.read_bytes()
    if len(raw) > MAX_BYTES:
        raise SnapshotError("core_state_invalid")
    try:
        state = json.loads(raw.decode("utf-8-sig"))
        _check_state(state)
        canonical = _canonical(state)
    except Exception:
        raise SnapshotError("core_state_invalid") from None
    return raw, _hash(canonical)


def _archive_stats(db, *, validate_fills):
    db.row_factory = sqlite3.Row
    schema = tuple(tuple(row) for row in _schema(db))
    if schema != _reviewed_schema():
        raise SnapshotError("archive_schema_invalid")
    counts = {name: db.execute("SELECT COUNT(*) FROM " + name).fetchone()[0] for name in TABLES}
    cutoff = {name: db.execute("SELECT COALESCE(MAX(sequence),0) FROM " + name).fetchone()[0]
              for name in ("events", "samples", "datasets")}
    rows = db.execute("SELECT payload_hash FROM events WHERE kind='broker_fill_activity' ORDER BY sequence").fetchall()
    result = {"table_counts": counts, "evidence_cutoff_sequence": cutoff,
              "raw_fill_event_rows": len(rows)}
    if not validate_fills:
        return result
    if ([tuple(row) for row in db.execute("PRAGMA integrity_check")] != [("ok",)]
            or db.execute("PRAGMA foreign_key_check").fetchall()):
        raise SnapshotError("archive_integrity_failed")
    versions, activities = set(), set()
    try:
        for row in rows:
            event = ResearchArchive._get(db, row[0])
            activity = event["activity"]
            identity, version = event["activity_id"], event["activity_hash"]
            if (not isinstance(identity, str) or not identity
                    or not isinstance(activity, dict) or activity.get("id") != identity
                    or event.get("source") != "alpaca_account_activities_FILL"
                    or activity.get("activity_type") != "FILL"
                    or not isinstance(version, str) or version != fingerprint(activity)):
                raise ValueError
            activities.add(identity)
            versions.add((identity, version))
    except Exception:
        raise SnapshotError("raw_fill_evidence_invalid") from None
    result.update(raw_fill_versions=len(versions), distinct_fill_activity_ids=len(activities),
                  correction_versions=len(versions) - len(activities))
    return result


def _backup(source, target):
    _regular(source)
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

    def progress(status, remaining, total):
        if status in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
            raise SnapshotError("archive_busy")

    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=0)) as original:
        original.execute("PRAGMA query_only=ON")
        original.execute("PRAGMA trusted_schema=OFF")
        original.execute("BEGIN")
        original.execute("SELECT COUNT(*) FROM sqlite_schema").fetchone()
        source_stats = _archive_stats(original, validate_fills=False)
        # The destination directory is new and exclusively created by this call.
        with target.open("xb"):
            pass
        os.chmod(target, 0o600)
        with closing(sqlite3.connect(target, timeout=0)) as copied:
            original.backup(copied, pages=512, progress=progress, sleep=0)
        original.rollback()
    return source_stats


def _paths(runtime, destination):
    runtime, destination = Path(runtime), Path(destination)
    if not runtime.is_absolute() or not destination.is_absolute():
        raise SnapshotError("absolute_paths_required")
    _no_links(runtime)
    _no_links(destination)
    runtime, destination = runtime.resolve(), destination.resolve()
    if not runtime.is_dir() or not destination.parent.is_dir() or destination.exists():
        raise SnapshotError("new_destination_required")
    if (destination.is_relative_to(runtime) or runtime.is_relative_to(destination)
            or destination.is_relative_to(_SOURCE_ROOT)):
        raise SnapshotError("private_separate_destination_required")
    lock = runtime / "runner.lock"
    if _regular(lock).st_size < 1:
        raise SnapshotError("existing_runtime_lock_required")
    return runtime, destination


def prepare_snapshot(runtime, destination):
    """Return a small receipt. Manifest is written last; failures keep partials.

    A prepared snapshot is only readonly_validation. It does not prove a stopped
    worker, exclusive cloud ownership, fresh broker reconciliation or cutover.
    """
    try:
        runtime, destination = _paths(runtime, destination)
        lock = process_lock(runtime)
        try:
            lock.__enter__()
        except RuntimeError:
            return {"status": "pending_lock", "snapshot_kind": "readonly_validation",
                    "trading_enabled": False, "cutover_proof": False}
        started = _now()
        try:
            destination.mkdir(mode=0o700, exist_ok=False)
            raw_state, state_digest = _state_bytes(runtime / "state.json")
            _new_file(destination / "state.json", raw_state)
            source_stats = _backup(runtime / "archive" / "research.sqlite3",
                                   destination / "archive" / "research.sqlite3")
            names, excluded = cloud_auxiliary._paths(runtime)
            auxiliaries, total = [], 0
            for name in names:
                _regular(runtime / name)
                raw = cloud_auxiliary._read_safe(runtime, name)
                total += len(raw)
                if total > cloud_auxiliary.MAX_TOTAL_BYTES:
                    raise SnapshotError("auxiliary_capacity_exceeded")
                _new_file(destination / name, raw)
                auxiliaries.append({"path": name, "sha256": _hash(raw), "bytes": len(raw)})
            if _state_bytes(runtime / "state.json")[0] != raw_state:
                raise SnapshotError("source_changed_during_snapshot")
            if cloud_auxiliary._paths(runtime) != (names, excluded):
                raise SnapshotError("source_changed_during_snapshot")
            for item in auxiliaries:
                if _hash(cloud_auxiliary._read_safe(runtime, item["path"])) != item["sha256"]:
                    raise SnapshotError("source_changed_during_snapshot")
        finally:
            lock.__exit__(None, None, None)
        captured = _now()
        archive_path = destination / "archive" / "research.sqlite3"
        with closing(sqlite3.connect(archive_path.as_uri() + "?mode=ro", uri=True, timeout=0)) as copied:
            copied.execute("PRAGMA query_only=ON")
            copied.execute("PRAGMA trusted_schema=OFF")
            verified = _archive_stats(copied, validate_fills=True)
        if any(verified[key] != value for key, value in source_stats.items()):
            raise SnapshotError("archive_copy_mismatch")
        if _state_bytes(destination / "state.json") != (raw_state, state_digest):
            raise SnapshotError("core_copy_mismatch")
        files = [{"path": "state.json", "sha256": _hash(raw_state), "bytes": len(raw_state)},
                 {"path": "archive/research.sqlite3", "sha256": _file_hash(archive_path),
                  "bytes": archive_path.stat().st_size}]
        for item in auxiliaries:
            path = destination / item["path"]
            if path.stat().st_size != item["bytes"] or _file_hash(path) != item["sha256"]:
                raise SnapshotError("auxiliary_copy_mismatch")
        manifest = {"format_version": 1, "snapshot_kind": "readonly_validation",
                    "cutover_proof": False, "trading_enabled": False,
                    "fresh_cutover_snapshot_required": True,
                    "capture_started_at": started, "capture_finished_at": captured,
                    "verified_at": _now(), "core_state_sha256": state_digest,
                    "archive": verified, "files": files + auxiliaries,
                    "auxiliary_excluded": excluded,
                    "auxiliary_atomic_with_archive": False,
                    "fill_count_semantics": "Versions by activity_id and activity_hash; correction versions are not additional executions."}
        raw_manifest = _json(manifest)
        _new_file(destination / "manifest.json.partial", raw_manifest)
        os.replace(destination / "manifest.json.partial", destination / "manifest.json")
        if (destination / "manifest.json").read_bytes() != raw_manifest:
            raise SnapshotError("manifest_readback_failed")
        return {"status": "prepared", "snapshot_kind": "readonly_validation",
                "trading_enabled": False, "cutover_proof": False,
                "manifest_sha256": _hash(raw_manifest), "file_count": len(manifest["files"]),
                "table_counts": verified["table_counts"], "raw_fill_versions": verified["raw_fill_versions"]}
    except SnapshotError:
        raise
    except Exception:
        raise SnapshotError("snapshot_preparation_failed") from None


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    try:
        if len(args) != 4 or set(args[::2]) != {"--runtime", "--destination"}:
            raise SnapshotError("invalid_arguments")
        options = dict(zip(args[::2], args[1::2]))
        result = prepare_snapshot(options["--runtime"], options["--destination"])
    except SnapshotError as error:
        result = {"status": "failed", "error_category": str(error),
                  "snapshot_kind": "readonly_validation", "trading_enabled": False, "cutover_proof": False}
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
