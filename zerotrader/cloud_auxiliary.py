"""Private auxiliary state carried inside the append-only research archive.

Only the explicit paths below are read. File bytes live in content-addressed
events; the latest manifest names the complete current set. Nothing here reads
credentials, exports, worker metadata, or arbitrary runtime files. A manifest is
durable only when its containing archive bundle has been confirmed remotely.
"""
from contextlib import closing
import base64
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sqlite3
import stat

from .archive import ResearchArchive, redact


FILE_KIND = "cloud_auxiliary_file_v1"
MANIFEST_KIND = "cloud_auxiliary_manifest_v1"
_TOP = frozenset({
    "fill-archive-cursor.json", "archive-backfill.json", "research.json",
    "monitor-state.json", "selection.json", "latest.json", "equity-history.json",
    "daily.json", "intraday.json", "data_meta.json", "paper-probe.json",
    "cloud-cutover.json",
})
_REVIEW_SUFFIXES = frozenset({".json", ".txt", ".md", ".csv", ".py", ".sql"})
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_PART = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}\Z")
MAX_FILE_BYTES = 128 * 1024 * 1024
MAX_TOTAL_BYTES = 512 * 1024 * 1024
MAX_FILES = 10000


class AuxiliaryError(RuntimeError):
    """Fixed diagnostics intentionally omit file content and private paths."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


def _digest(raw):
    return hashlib.sha256(raw).hexdigest()


def _excluded_reason(name):
    parts = PurePosixPath(name).parts
    if len(parts) >= 3 and parts[0] == "reviews" and parts[1].startswith("peer-code-"):
        return "historical_peer_source_attachment_retained_locally"
    if name == "reviews/claude-small-account-review-20261002-164726.json":
        return "historical_peer_authorization_receipt_retained_locally"
    return None


def _valid_path_syntax(name):
    if not isinstance(name, str) or "\\" in name or ":" in name:
        return False
    path = PurePosixPath(name)
    if path.is_absolute() or path.as_posix() != name or any(
            part in (".", "..") or not _PART.fullmatch(part) for part in path.parts):
        return False
    return True


def allowed_path(name):
    if not _valid_path_syntax(name):
        return False
    path = PurePosixPath(name)
    if _excluded_reason(name):
        return False
    if name in _TOP:
        return True
    if len(path.parts) == 2 and path.parts[0] == "research-history":
        return path.suffix == ".json"
    return len(path.parts) >= 2 and path.parts[0] == "reviews" and path.suffix in _REVIEW_SUFFIXES


def _safe_path(root, name):
    if not allowed_path(name):
        raise AuxiliaryError("Auxiliary path is outside the reviewed allowlist.")
    root = Path(root).resolve()
    path = root.joinpath(*PurePosixPath(name).parts)
    current = path
    while current != root:
        if current.is_symlink() or (hasattr(current, "is_junction") and current.is_junction()):
            raise AuxiliaryError("Auxiliary links are forbidden.")
        current = current.parent
    if not path.resolve().is_relative_to(root):
        raise AuxiliaryError("Auxiliary path escapes its private runtime.")
    return path


def _paths(root):
    root = Path(root).resolve()
    result = [name for name in sorted(_TOP) if _safe_path(root, name).exists()]
    excluded = []
    # Enumerate only the two reviewed subdirectories, never the runtime tree.
    for folder in ("research-history", "reviews"):
        directory = root / folder
        if directory.is_symlink() or (hasattr(directory, "is_junction") and directory.is_junction()):
            raise AuxiliaryError("Auxiliary links are forbidden.")
        if not directory.exists():
            continue
        candidates = directory.glob("*.json") if folder == "research-history" else directory.rglob("*")
        for path in candidates:
            name = path.relative_to(root).as_posix()
            if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
                raise AuxiliaryError("Auxiliary links are forbidden.")
            if path.is_file():
                reason = _excluded_reason(name)
                if reason:
                    excluded.append({"path": name, "reason": reason})
                elif allowed_path(name):
                    result.append(name)
    if len(result) > MAX_FILES:
        raise AuxiliaryError("Auxiliary file count exceeds the reviewed capacity.")
    return sorted(result), sorted(excluded, key=lambda item: item["path"])


def _read_safe(root, name):
    path = _safe_path(root, name)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_FILE_BYTES:
        raise AuxiliaryError("Auxiliary file exceeds the reviewed capacity or type.")
    raw = path.read_bytes()
    if len(raw) != info.st_size or len(raw) > MAX_FILE_BYTES:
        raise AuxiliaryError("Auxiliary file changed while being captured.")
    try:
        text = raw.decode("utf-8-sig")
        value = json.loads(text) if name.endswith(".json") else text
        if redact(value) != value:
            raise ValueError
    except (ValueError, UnicodeError, TypeError, RecursionError):
        raise AuxiliaryError("Auxiliary content is invalid or contains credential fields.") from None
    return raw


def _read_manifest(db):
    row = db.execute("SELECT payload_hash FROM events WHERE kind=? ORDER BY sequence DESC LIMIT 1",
                     (MANIFEST_KIND,)).fetchone()
    return ResearchArchive._get(db, row[0]) if row else None


def capture_auxiliary(archive, run_id, directory):
    """Append changed file versions and a full manifest; never delete history.

    Call immediately BEFORE a bundle commit. In particular the FILL cursor is
    never uploaded independently of the broker activity records it acknowledges.
    """
    files, total = [], 0
    paths, excluded = _paths(directory)
    with archive._connect() as db:
        previous = _read_manifest(db)
    if previous:
        # Excluded historical attachments are not present in a cold runtime.
        # Preserve the disclosure across later snapshots without reading them.
        excluded = sorted({item["path"]: item for item in previous.get("excluded", []) + excluded}.values(),
                          key=lambda item: item["path"])
    for name in paths:
        raw = _read_safe(directory, name)
        total += len(raw)
        if total > MAX_TOTAL_BYTES:
            raise AuxiliaryError("Auxiliary bytes exceed the reviewed capacity.")
        payload = {"version": 1, "path": name, "sha256": _digest(raw),
                   "bytes": len(raw), "base64": base64.b64encode(raw).decode("ascii")}
        payload_hash = _digest(_canonical(payload))
        archive.event_once(run_id, FILE_KIND, payload,
                           key="cloud-aux-file-v1:" + payload_hash)
        files.append({"path": name, "sha256": payload["sha256"],
                      "bytes": len(raw), "payload_hash": payload_hash})
    manifest = {"version": 1, "files": files, "total_bytes": total, "excluded": excluded}
    if previous != manifest:
        # Do not deduplicate manifests globally: A -> B -> A must restore A.
        archive.event(run_id, MANIFEST_KIND, manifest)
    return _digest(_canonical(manifest))


def _verified_files(db, manifest):
    if (not isinstance(manifest, dict) or set(manifest) != {"version", "files", "total_bytes", "excluded"}
            or manifest["version"] != 1 or type(manifest["version"]) is not int
            or not isinstance(manifest["files"], list) or len(manifest["files"]) > MAX_FILES
            or type(manifest["total_bytes"]) is not int or not isinstance(manifest["excluded"], list)
            or len(manifest["excluded"]) > MAX_FILES):
        raise AuxiliaryError("A verified auxiliary manifest is required.")
    for excluded in manifest["excluded"]:
        if (not isinstance(excluded, dict) or set(excluded) != {"path", "reason"}
                or not _valid_path_syntax(excluded["path"]) or not _excluded_reason(excluded["path"])
                or _excluded_reason(excluded["path"]) != excluded["reason"]):
            raise AuxiliaryError("Auxiliary exclusion is outside the reviewed exception list.")
    result, names, total = [], set(), 0
    for item in manifest["files"]:
        if (not isinstance(item, dict) or set(item) != {"path", "sha256", "bytes", "payload_hash"}
                or not allowed_path(item["path"]) or item["path"].casefold() in names
                or not isinstance(item["sha256"], str) or not _HASH.fullmatch(item["sha256"])
                or not isinstance(item["payload_hash"], str) or not _HASH.fullmatch(item["payload_hash"])
                or type(item["bytes"]) is not int or not 0 <= item["bytes"] <= MAX_FILE_BYTES):
            raise AuxiliaryError("Auxiliary manifest entry is invalid.")
        names.add(item["path"].casefold())
        # Require an actual append-only file event, not any arbitrary blob.
        row = db.execute("SELECT 1 FROM events WHERE kind=? AND payload_hash=? LIMIT 1",
                         (FILE_KIND, item["payload_hash"])).fetchone()
        if row is None:
            raise AuxiliaryError("Auxiliary file evidence is missing.")
        payload = ResearchArchive._get(db, item["payload_hash"])
        if (not isinstance(payload, dict) or set(payload) != {"version", "path", "sha256", "bytes", "base64"}
                or type(payload["version"]) is not int or payload["version"] != 1
                or any(payload[key] != item[key] for key in ("path", "sha256", "bytes"))
                or not isinstance(payload["base64"], str)
                or len(payload["base64"]) > 4 * ((MAX_FILE_BYTES + 2) // 3)):
            raise AuxiliaryError("Auxiliary file evidence differs from its manifest.")
        try:
            raw = base64.b64decode(payload["base64"], validate=True)
        except (ValueError, UnicodeError):
            raise AuxiliaryError("Auxiliary encoding is invalid.") from None
        if len(raw) != item["bytes"] or _digest(raw) != item["sha256"]:
            raise AuxiliaryError("Auxiliary file hash does not match.")
        total += len(raw)
        if total > MAX_TOTAL_BYTES:
            raise AuxiliaryError("Auxiliary bytes exceed the reviewed capacity.")
        result.append((item["path"], raw))
    if total != manifest["total_bytes"]:
        raise AuxiliaryError("Auxiliary byte count does not match.")
    return result


def restore_auxiliary(archive_path, directory):
    """Restore verified bytes to absent allowlisted files in a NEW runtime.

    The caller must first restore the archive from a verified private bundle.
    Validate the entire manifest before writing any file. Partial I/O failures
    must abort the coordinator, never trigger trading from a partial restore.
    """
    try:
        uri = Path(archive_path).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            manifest = _read_manifest(db)
            files = _verified_files(db, manifest)
        root = Path(directory).resolve()
        root.mkdir(parents=True, exist_ok=True)
        targets = [(_safe_path(root, name), raw) for name, raw in files]
        if any(path.exists() for path, _ in targets):
            raise AuxiliaryError("Auxiliary restore requires absent destination files.")
        for path, raw in targets:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as stream:
                stream.write(raw)
        return {"files": len(files), "total_bytes": manifest["total_bytes"],
                "manifest_sha256": _digest(_canonical(manifest)), "excluded": manifest["excluded"]}
    except AuxiliaryError:
        raise
    except (OSError, sqlite3.Error, KeyError, ValueError, TypeError, RecursionError):
        raise AuxiliaryError("Private auxiliary restoration failed.") from None
