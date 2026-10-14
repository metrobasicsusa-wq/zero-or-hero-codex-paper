"""Experimental private, small-state Git checkpoint; not used by the trading engine.

Only a dedicated append-only branch, with deletion/rewrites prohibited for ALL
writers, provides the expected-head conflict semantics below. Plain Git push is
not a strict compare-and-swap against an administrator resetting the branch.
This is neither an execution lease nor a broker retry/reconciliation mechanism.
Optional immutable package files can be committed with core state; the separate
bundle layer must verify archive closure. This store alone does not solve exit
availability, authenticate a cloud deployment, or authorize one. Callers must
never submit an order until its
intent checkpoint is confirmed, and must reconcile its original client_order_id
after interruption instead of blindly repeating a broker write.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
from urllib.parse import urlsplit


CHECKPOINT_PATH = "checkpoint/core-state.json"
MAX_BYTES = 1024 * 1024
MAX_BUNDLE_FILE_BYTES = 8 * 1024 * 1024
MAX_BUNDLE_WRITE_BYTES = 1024 * 1024 * 1024
ARCHIVE_INDEX_PATH = "checkpoint/archive-index.json"
_PACKAGE_PATH = re.compile(r"archive/packages/[0-9a-f]{64}/(?:manifest\.json|chunk-[0-9]{6}-[0-9a-f]{64}\.gz)\Z")
_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_STATE_FIELDS = frozenset({
    "version", "account_id", "equity_peak", "halted", "paused", "owners",
    "intents", "fills", "strategies", "last_research", "last_cycle", "events",
    "known_order_ids", "pending_research",
})
_REQUIRED_FIELDS = _STATE_FIELDS - {"known_order_ids", "pending_research"}
_SECRET_FIELDS = frozenset({
    "key", "secret", "apikey", "apisecret", "token", "password", "authorization",
    "credentials", "credential", "accesstoken", "refreshtoken", "privatekey",
    "apcaapikeyid", "apcaapisecretkey",
})


class CheckpointError(RuntimeError):
    """Errors intentionally contain no subprocess output, state or remote URL."""


class CheckpointMissing(CheckpointError):
    pass


class CheckpointConflict(CheckpointError):
    pass


class CheckpointCorrupt(CheckpointError):
    pass


class CheckpointInvalid(CheckpointError):
    pass


@dataclass(frozen=True)
class CheckpointReceipt:
    commit: str
    previous_commit: str | None
    state_sha256: str


@dataclass(frozen=True)
class LoadedCheckpoint:
    receipt: CheckpointReceipt
    state: dict = field(repr=False)


class CheckpointUncertain(CheckpointError):
    def __init__(self, receipt: CheckpointReceipt):
        super().__init__("Checkpoint outcome is unconfirmed; reread before any broker action.")
        self.receipt = receipt


def _canonical(value):
    try:
        result = json.dumps(value, ensure_ascii=False, allow_nan=False,
                            sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, RecursionError, UnicodeError):
        raise CheckpointInvalid("Checkpoint must be finite JSON data.") from None
    if len(result) > MAX_BYTES:
        raise CheckpointInvalid("Checkpoint exceeds the small-state size limit.")
    return result


def _check_state(state):
    if (not isinstance(state, dict) or set(state) - _STATE_FIELDS
            or _REQUIRED_FIELDS - set(state)):
        raise CheckpointInvalid("Complete supported core-state fields are required.")
    if type(state["version"]) is not int or state["version"] != 1:
        raise CheckpointInvalid("Unsupported core-state version.")
    if (any(not isinstance(state[name], dict) for name in ("owners", "fills", "strategies"))
            or not isinstance(state["events"], list)
            or any(state[name] is not None and not isinstance(state[name], str)
                   for name in ("last_research", "last_cycle"))
            or ("known_order_ids" in state and not isinstance(state["known_order_ids"], list))
            or ("pending_research" in state and not isinstance(state["pending_research"], dict))):
        raise CheckpointInvalid("Invalid core-state field types.")
    if not isinstance(state.get("account_id"), str) or not state["account_id"].strip():
        raise CheckpointInvalid("An existing bound account state is required.")
    peak = state.get("equity_peak")
    if isinstance(peak, bool) or not isinstance(peak, (float, int)) or not math.isfinite(peak) or peak <= 0:
        raise CheckpointInvalid("A finite positive equity peak is required.")
    if not isinstance(state.get("paused"), bool):
        raise CheckpointInvalid("An explicit pause state is required.")
    if "halted" not in state or (state["halted"] is not None and not isinstance(state["halted"], str)):
        raise CheckpointInvalid("An explicit halt state is required.")
    if not isinstance(state.get("intents"), dict):
        raise CheckpointInvalid("Persisted order intents are required.")
    if any(not isinstance(order_id, str) or not order_id for order_id in state.get("known_order_ids", [])):
        raise CheckpointInvalid("Known order IDs must be nonempty strings.")
    pending = [state]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            for key, child in value.items():
                if not isinstance(key, str):
                    raise CheckpointInvalid("JSON field names must be strings.")
                normalized = re.sub(r"[^a-z0-9]", "", key.lower())
                if normalized in _SECRET_FIELDS:
                    raise CheckpointInvalid("Credential fields are forbidden in checkpoints.")
                pending.append(child)
        elif isinstance(value, list):
            pending.extend(value)
    for client_id, intent in state["intents"].items():
        if not client_id or not isinstance(intent, dict) or not isinstance(intent.get("payload"), dict):
            raise CheckpointInvalid("Invalid persisted order intent.")
        if intent["payload"].get("client_order_id") != client_id:
            raise CheckpointInvalid("Order intent must retain its original client order ID.")
        if any(not isinstance(intent.get(name), str) or not intent[name]
               for name in ("sleeve", "status", "created")):
            raise CheckpointInvalid("Order intent attribution, status and creation time are required.")


def _transition(previous, proposed):
    if previous["account_id"] != proposed["account_id"]:
        raise CheckpointInvalid("Account binding cannot change.")
    if proposed["equity_peak"] < previous["equity_peak"]:
        raise CheckpointInvalid("Observed equity peak cannot decrease.")
    if previous["halted"] and proposed["halted"] != previous["halted"]:
        raise CheckpointInvalid("A latched halt cannot be changed by this store.")
    if previous["paused"] and not proposed["paused"]:
        raise CheckpointInvalid("A pause cannot be cleared by this store.")
    if not set(previous.get("known_order_ids", [])).issubset(proposed.get("known_order_ids", [])):
        raise CheckpointInvalid("Previously known order IDs cannot be removed.")
    for client_id, intent in previous["intents"].items():
        next_intent = proposed["intents"].get(client_id)
        if next_intent is None or next_intent.get("payload") != intent["payload"]:
            raise CheckpointInvalid("Existing order intents cannot be removed or rewritten.")
        for name in ("order_id", "sleeve", "exit_for", "created"):
            if name in intent and (name not in next_intent or next_intent[name] != intent[name]):
                raise CheckpointInvalid("Persisted order lineage cannot be removed or rewritten.")


def _oid(value):
    return isinstance(value, str) and _OID.fullmatch(value) is not None


def _bundle_path(value):
    if not isinstance(value, str) or not (value == ARCHIVE_INDEX_PATH or _PACKAGE_PATH.fullmatch(value)):
        raise CheckpointInvalid("Only the archive index and immutable archive package paths are allowed.")
    return value


class GitCheckpointStore:
    """Explicit load/bootstrap/commit/confirm operations, no automatic initialization.

    The remote MUST be private (this offline foundation cannot verify visibility).
    Only absolute local paths or HTTPS URLs without userinfo/query/fragment are
    accepted. No production private-HTTPS authentication is implemented here:
    inherited Git config/helpers/trace variables are disabled. Authentication
    and remote branch protection are separate, unfinished deployment work.
    Raw state goes only to Git's stdin, never command arguments or diagnostics.
    """

    def __init__(self, remote, branch="core-checkpoints", *, git_executable="git", timeout_seconds=30,
                 initial_fetch_timeout_seconds=None):
        remote = os.fspath(remote)
        try:
            parsed = urlsplit(remote)
            valid_https = (parsed.scheme == "https" and parsed.hostname and
                           not parsed.username and not parsed.password and
                           not parsed.query and not parsed.fragment and "@" not in parsed.netloc)
            valid_local = Path(remote).is_absolute() and not remote.startswith(("//", "\\\\"))
        except (ValueError, TypeError):
            valid_https = valid_local = False
        if (not remote or any(ord(c) < 32 for c in remote) or not (valid_https or valid_local)):
            raise CheckpointInvalid("Remote must be an absolute local path or credential-free HTTPS URL.")
        if (not isinstance(branch, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", branch)
                or ".." in branch or "//" in branch or branch.endswith(("/", ".", ".lock"))
                or any(part.startswith(".") or part.endswith(".lock") for part in branch.split("/"))):
            raise CheckpointInvalid("Invalid checkpoint branch.")
        if not isinstance(timeout_seconds, (int, float)) or timeout_seconds <= 0 or not math.isfinite(timeout_seconds):
            raise CheckpointInvalid("A finite positive Git timeout is required.")
        if (initial_fetch_timeout_seconds is not None
                and (type(initial_fetch_timeout_seconds) not in (int, float)
                     or initial_fetch_timeout_seconds <= 0 or not math.isfinite(initial_fetch_timeout_seconds))):
            raise CheckpointInvalid("A finite positive initial fetch timeout is required.")
        self._remote = remote
        self._ref = "refs/heads/" + branch
        self._git_executable = git_executable
        self._timeout = timeout_seconds
        self._initial_fetch_timeout = initial_fetch_timeout_seconds
        self._initial_fetch_completed = False
        self._object_cache = None
        self._cache_lock = threading.RLock()

    def _git_environment(self):
        env = {key: value for key, value in os.environ.items()
               if not key.upper().startswith(("GIT_", "GCM_TRACE"))}
        env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                   GIT_TERMINAL_PROMPT="0", GIT_AUTHOR_NAME="Checkpoint Writer",
                   GIT_AUTHOR_EMAIL="checkpoint@invalid.example",
                   GIT_COMMITTER_NAME="Checkpoint Writer",
                   GIT_COMMITTER_EMAIL="checkpoint@invalid.example")
        return env

    def _git(self, directory, *args, data=None):
        env = self._git_environment()
        # A fresh private object cache may need to receive the full historical
        # archive. Give only that initial read-only fetch its separate allowance;
        # intent/core writes and all other commands retain the normal timeout.
        timeout = self._timeout
        if args and args[0] == "fetch" and not self._initial_fetch_completed:
            timeout = max(timeout, self._initial_fetch_timeout or timeout)
        try:
            return subprocess.run(
                [self._git_executable, "-c", "core.hooksPath=" + str(Path(directory) / "no-hooks"),
                 "-c", "credential.helper=", *args], cwd=directory, env=env,
                input=data, capture_output=True, timeout=timeout, check=False,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
        except (OSError, subprocess.SubprocessError):
            raise CheckpointError("Git operation unavailable; checkpoint was not confirmed.") from None

    def _checked(self, directory, *args, data=None):
        result = self._git(directory, *args, data=data)
        if result.returncode:
            raise CheckpointError("Git operation failed; checkpoint was not confirmed.")
        return result.stdout

    @contextmanager
    def _repository(self):
        # Reuse private Git objects within one process. Otherwise every core
        # save would download the entire archive history again. Every operation
        # still verifies remote HEAD and exact commit contents; this is not a
        # cached claim that a previous remote write succeeded.
        with self._cache_lock:
            if self._object_cache is None:
                temporary = tempfile.TemporaryDirectory(prefix="zoh-checkpoint-")
                try:
                    self._checked(temporary.name, "init", "--quiet")
                except Exception:
                    temporary.cleanup()
                    raise
                self._object_cache = temporary
                self._initial_fetch_completed = False
            yield self._object_cache.name

    def close(self):
        """Discard this process's private object cache after the cycle/job."""
        with self._cache_lock:
            if self._object_cache is not None:
                self._object_cache.cleanup()
                self._object_cache = None
            self._initial_fetch_completed = False

    def __del__(self):
        temporary = getattr(self, "_object_cache", None)
        if temporary is not None:
            try:
                temporary.cleanup()
            except OSError:
                pass

    def _head(self, directory):
        result = self._git(directory, "ls-remote", "--exit-code", "--", self._remote, self._ref)
        if result.returncode == 2:
            return None
        if result.returncode:
            raise CheckpointError("Remote checkpoint head is unavailable.")
        try:
            lines = result.stdout.decode("ascii").strip().splitlines()
            commit, ref = lines[0].split("\t")
            if len(lines) != 1 or ref != self._ref or not _oid(commit):
                raise ValueError
        except (ValueError, UnicodeError, IndexError):
            raise CheckpointCorrupt("Invalid remote checkpoint head.") from None
        return commit

    def _fetch(self, directory, commit):
        self._checked(directory, "fetch", "--quiet", "--no-tags", "--no-recurse-submodules",
                      "--", self._remote, commit)
        fetched = self._checked(directory, "rev-parse", "FETCH_HEAD").decode("ascii").strip()
        if fetched != commit:
            raise CheckpointCorrupt("Fetched checkpoint does not match the observed head.")
        self._initial_fetch_completed = True

    def _read(self, directory, commit):
        result = self._git(directory, "show", commit + ":" + CHECKPOINT_PATH)
        if result.returncode:
            raise CheckpointMissing("The remote branch has no readable core checkpoint.")
        try:
            raw = result.stdout
            if len(raw) > MAX_BYTES:
                raise ValueError
            envelope = json.loads(raw)
            if (not isinstance(envelope, dict) or set(envelope) != {
                    "version", "previous_commit", "state_sha256", "state"} or envelope["version"] != 1):
                raise ValueError
            previous = envelope["previous_commit"]
            if previous is not None and not _oid(previous):
                raise ValueError
            state = envelope["state"]
            state_bytes = _canonical(state)
            _check_state(state)
            digest = hashlib.sha256(state_bytes).hexdigest()
            if envelope["state_sha256"] != digest or raw != _canonical(envelope):
                raise ValueError
            lineage = self._checked(directory, "rev-list", "--parents", "-n", "1", commit).decode("ascii").split()
            if lineage != [commit, *([previous] if previous is not None else [])]:
                raise ValueError
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, CheckpointInvalid):
            raise CheckpointCorrupt("Checkpoint content or SHA-256 verification failed.") from None
        return LoadedCheckpoint(CheckpointReceipt(commit, previous, digest), state)

    def load(self):
        """Read an existing checkpoint. Missing state is an error, never new_state()."""
        with self._repository() as directory:
            head = self._head(directory)
            if head is None:
                raise CheckpointMissing("Checkpoint branch is missing; explicit bootstrap is required.")
            self._fetch(directory, head)
            loaded = self._read(directory, head)
            if self._head(directory) != head:
                raise CheckpointConflict("Checkpoint head changed during the read; load again.")
            return loaded

    def confirm(self, receipt):
        """Reread a previously uncertain commit; never retries a push or broker write.

        A newer head is a conflict, even if it descends from this receipt: load
        and reconcile the latest state explicitly instead of accepting stale data.
        """
        if (not isinstance(receipt, CheckpointReceipt) or not _oid(receipt.commit)
                or (receipt.previous_commit is not None and not _oid(receipt.previous_commit))
                or not isinstance(receipt.state_sha256, str) or not _HASH.fullmatch(receipt.state_sha256)):
            raise CheckpointInvalid("Invalid checkpoint receipt.")
        loaded = self.load()
        if loaded.receipt.commit != receipt.commit:
            raise CheckpointConflict("Remote head differs from the receipt; load and reconcile latest state.")
        core_receipt = CheckpointReceipt(receipt.commit, receipt.previous_commit, receipt.state_sha256)
        if loaded.receipt != core_receipt:
            raise CheckpointCorrupt("Receipt does not match the remote checkpoint content.")
        return loaded

    def read_files(self, receipt, paths):
        """Read exact package bytes only while this receipt remains current.

        Package validity/reference closure is checked by the archive/bundle layer.
        No working-tree checkout, filters, symlinks or archive extraction is used.
        """
        if not isinstance(paths, (list, tuple)):
            raise CheckpointInvalid("A unique list of archive paths is required.")
        for path in paths:
            _bundle_path(path)
        if len(paths) != len(set(paths)):
            raise CheckpointInvalid("A unique list of archive paths is required.")
        self.confirm(receipt)
        result = {}
        total = 0
        with self._repository() as directory:
            if self._head(directory) != receipt.commit:
                raise CheckpointConflict("Checkpoint changed before archive readback.")
            self._fetch(directory, receipt.commit)
            for path in paths:
                spec = receipt.commit + ":" + path
                kind = self._git(directory, "cat-file", "-t", spec)
                if kind.returncode or kind.stdout.strip() != b"blob":
                    raise CheckpointCorrupt("Required archive file is missing or invalid.")
                size = self._checked(directory, "cat-file", "-s", spec)
                try:
                    length = int(size.strip())
                except ValueError:
                    raise CheckpointCorrupt("Invalid archive file size.") from None
                limit = MAX_BYTES if path == ARCHIVE_INDEX_PATH else MAX_BUNDLE_FILE_BYTES
                if not 0 <= length <= limit or total + length > MAX_BUNDLE_WRITE_BYTES:
                    raise CheckpointCorrupt("Archive readback exceeds its size limit.")
                content = self._checked(directory, "cat-file", "blob", spec)
                if len(content) != length:
                    raise CheckpointCorrupt("Archive file readback was incomplete.")
                result[path] = content
                total += length
            if self._head(directory) != receipt.commit:
                raise CheckpointConflict("Checkpoint changed during archive readback.")
        return result

    def prove_files_unchanged(self, receipt, previous_receipt, paths):
        """Verify immutable Git mode/blob identities across a descendant head.

        This permits reuse of already verified local archive bytes; it does not
        validate unverified cache contents or make an execution lease.
        """
        if not isinstance(paths, (list, tuple)):
            raise CheckpointInvalid("An explicit list of archive paths is required.")
        for path in paths:
            _bundle_path(path)
        if not isinstance(previous_receipt, CheckpointReceipt) or not _oid(previous_receipt.commit):
            raise CheckpointInvalid("A previous archive receipt is required.")
        self.confirm(receipt)
        with self._repository() as directory:
            if self._head(directory) != receipt.commit:
                raise CheckpointConflict("Checkpoint changed before archive lineage check.")
            self._fetch(directory, receipt.commit)
            previous_core = CheckpointReceipt(previous_receipt.commit, previous_receipt.previous_commit,
                                              previous_receipt.state_sha256)
            if self._read(directory, previous_receipt.commit).receipt != previous_core:
                raise CheckpointCorrupt("Previous archive receipt did not match its commit.")
            result = self._git(directory, "merge-base", "--is-ancestor", previous_receipt.commit, receipt.commit)
            if result.returncode:
                raise CheckpointConflict("Archive cache is not from an ancestor checkpoint.")
            def entries(commit):
                raw = self._checked(directory, "ls-tree", "-r", "-z", commit, "--", "archive/packages/", ARCHIVE_INDEX_PATH)
                result = {}
                try:
                    for line in raw.split(b"\0"):
                        if not line:
                            continue
                        info, name = line.split(b"\t", 1)
                        mode, kind, object_id = info.decode("ascii").split()
                        result[name.decode("utf-8")] = (mode, kind, object_id)
                except (ValueError, UnicodeError):
                    raise CheckpointCorrupt("Archive Git tree could not be verified.") from None
                return result
            old, new = entries(previous_receipt.commit), entries(receipt.commit)
            for path in paths:
                if (path not in old or path not in new or old[path] != new[path]
                        or old[path][:2] != ("100644", "blob")):
                    raise CheckpointCorrupt("A cached archive file changed or is missing.")
            if self._head(directory) != receipt.commit:
                raise CheckpointConflict("Checkpoint changed during archive lineage check.")
        return True

    def bootstrap(self, state, *, files=None):
        """Explicit store creation from supplied, bound state, only on a missing branch.

        This does not create/reset a trading account or migrate a live runtime.
        """
        return self._write(state, expected_head=None, files=files)

    def commit(self, state, *, expected_head, files=None):
        if not _oid(expected_head):
            raise CheckpointInvalid("An explicit valid expected head is required.")
        return self._write(state, expected_head=expected_head, files=files)

    def _push(self, directory, commit):
        # Deliberately no +refspec, --force or --force-with-lease.
        return self._git(directory, "push", "--porcelain", "--", self._remote, commit + ":" + self._ref)

    def _write(self, state, expected_head, files=None):
        if files is None:
            files = {}
        if not isinstance(files, dict):
            raise CheckpointInvalid("Archive additions must be a path-to-bytes mapping.")
        additions = dict(files)
        total = 0
        for path, data in additions.items():
            _bundle_path(path)
            limit = MAX_BYTES if path == ARCHIVE_INDEX_PATH else MAX_BUNDLE_FILE_BYTES
            if not isinstance(data, bytes) or len(data) > limit:
                raise CheckpointInvalid("Archive file bytes exceed the allowed size or type.")
            total += len(data)
        if total > MAX_BUNDLE_WRITE_BYTES:
            raise CheckpointInvalid("Archive additions exceed the bounded write size.")
        # Canonicalize/copy before validation, preventing caller mutation mid-write.
        state_bytes = _canonical(state)
        state = json.loads(state_bytes)
        _check_state(state)
        digest = hashlib.sha256(state_bytes).hexdigest()
        envelope = _canonical({"version": 1, "previous_commit": expected_head,
                               "state_sha256": digest, "state": state})
        with self._repository() as directory:
            if self._head(directory) != expected_head:
                raise CheckpointConflict("Remote checkpoint head differs from the expected head.")
            if expected_head is not None:
                self._fetch(directory, expected_head)
                _transition(self._read(directory, expected_head).state, state)
                self._checked(directory, "read-tree", expected_head)
            else:
                self._checked(directory, "read-tree", "--empty")
            for path, data in additions.items():
                if expected_head is not None and path != ARCHIVE_INDEX_PATH:
                    previous = self._git(directory, "cat-file", "-e", expected_head + ":" + path)
                    if previous.returncode == 0:
                        existing = self._checked(directory, "cat-file", "blob", expected_head + ":" + path)
                        if existing != data:
                            raise CheckpointInvalid("An existing archive package file cannot be rewritten.")
                        continue
                bundle_blob = self._checked(directory, "hash-object", "-w", "--stdin", data=data).decode("ascii").strip()
                self._checked(directory, "update-index", "--add", "--cacheinfo", "100644", bundle_blob, path)
            blob = self._checked(directory, "hash-object", "-w", "--stdin", data=envelope).decode("ascii").strip()
            self._checked(directory, "update-index", "--add", "--cacheinfo", "100644", blob, CHECKPOINT_PATH)
            tree = self._checked(directory, "write-tree").decode("ascii").strip()
            parents = ("-p", expected_head) if expected_head is not None else ()
            commit = self._checked(directory, "commit-tree", tree, *parents,
                                   "-m", "Save private core checkpoint").decode("ascii").strip()
            lineage = self._checked(directory, "rev-list", "--parents", "-n", "1", commit).decode("ascii").split()
            if not _oid(commit) or lineage != [commit, *([expected_head] if expected_head else [])]:
                raise CheckpointCorrupt("Candidate commit has an unexpected parent.")
            receipt = CheckpointReceipt(commit, expected_head, digest)
            try:
                self._push(directory, commit)
            except CheckpointError:
                # A failed response cannot distinguish rejection from accepted push.
                pass
            try:
                return self.confirm(receipt).receipt
            except CheckpointConflict:
                # Head still at expected may mean a rejected or delayed push. Fail
                # closed with a receipt, not an instruction to submit/retry an order.
                try:
                    current = self._head(directory)
                except CheckpointError:
                    raise CheckpointUncertain(receipt) from None
                if current != expected_head:
                    raise CheckpointConflict("Another checkpoint won the update; load and reconcile latest state.") from None
                raise CheckpointUncertain(receipt) from None
            except CheckpointError:
                raise CheckpointUncertain(receipt) from None
