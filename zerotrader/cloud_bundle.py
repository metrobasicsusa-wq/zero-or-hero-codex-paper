"""Offline private core-state + archive Git bundles; no production integration.

A bundle receipt is issued only after actual remote core/index/package readback.
The caller must finish writing all inputs, samples and the order_intent event
BEFORE invoking the entry barrier with this store. This module verifies durable
bytes and archive closure, not the trading meaning/completeness of those inputs.
Core-only protective-exit checkpoints may advance independently; they do not
produce entry evidence receipts. One coordinator must serialize both writers.

Fresh loads read the entire archive. Cached commits can avoid downloading old
packages when their Git tree identity is proved unchanged. The current archive
exporter still scans the database and verifies the full LOCAL chain each time;
memory and work scale with history. No latency, free capacity, authentication,
private visibility, execution lease or broker availability is promised here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from . import cloud_archive
from .cloud_checkpoint import (
    ARCHIVE_INDEX_PATH, MAX_BYTES, CheckpointConflict, CheckpointError,
    CheckpointReceipt, GitCheckpointStore,
)


_HASH = re.compile(r'[0-9a-f]{64}\Z')
_CHUNK = re.compile(r'chunk-[0-9]{6}-[0-9a-f]{64}\.gz\Z')
_READ_BATCH_FILES = 16  # Each core file is <=8 MiB; never request the entire history at once.
_INDEX_FIELDS = {'version', 'state_sha256', 'packages', 'manifest_sha256',
                 'snapshot_sha256', 'generation', 'counts'}


class BundleError(CheckpointError):
    """Fixed diagnostics never include private paths, state or archive values."""


class BundleCorrupt(BundleError):
    pass


class BundleUncertain(BundleError):
    def __init__(self, checkpoint):
        super().__init__('Bundle readback is unconfirmed; no entry is authorized.')
        # A core receipt alone is deliberately NOT a BundleReceipt.
        self.checkpoint = checkpoint


@dataclass(frozen=True)
class BundleReceipt(CheckpointReceipt):
    archive_manifest_sha256: str
    archive_snapshot_sha256: str
    archive_index_sha256: str
    generation: int

    @property
    def checkpoint(self):
        return CheckpointReceipt(self.commit, self.previous_commit, self.state_sha256)


@dataclass(frozen=True)
class LoadedBundle:
    receipt: BundleReceipt
    state: dict = field(repr=False)
    counts: dict


def _json(value):
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False,
                          allow_nan=False, separators=(',', ':')).encode('utf-8')
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise BundleCorrupt('Bundle metadata must be finite canonical JSON.') from None


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _digest(value):
    return isinstance(value, str) and _HASH.fullmatch(value) is not None


def _index(raw):
    try:
        index = json.loads(raw)
        if (len(raw) > MAX_BYTES or _json(index) != raw or not isinstance(index, dict)
                or set(index) != _INDEX_FIELDS or type(index['version']) is not int
                or index['version'] != 1):
            raise ValueError
        packages = index['packages']
        if (not isinstance(packages, list) or not packages
                or any(not _digest(value) for value in packages)
                or len(set(packages)) != len(packages)
                or index['manifest_sha256'] != packages[-1]
                or type(index['generation']) is not int
                or index['generation'] != len(packages) - 1
                or not _digest(index['state_sha256']) or not _digest(index['snapshot_sha256'])
                or not isinstance(index['counts'], dict)
                or set(index['counts']) != set(cloud_archive.TABLES)
                or any(type(count) is not int or count < 0 for count in index['counts'].values())):
            raise ValueError
        return index
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise BundleCorrupt('Bundle archive index is invalid.') from None


def _prefix(digest):
    return 'archive/packages/' + digest + '/'


class GitBundleStore:
    """Explicit bootstrap/commit/load/confirm/restore, with no default state.

    Supply a dedicated private GitCheckpointStore. Its append-only branch and
    writer restrictions also apply here. close() removes only this object's
    temporary verified cache, never any remote package or supplied database.
    """

    def __init__(self, checkpoint_store: GitCheckpointStore, *, chunk_limit=cloud_archive.MAX_CHUNK_BYTES):
        if type(chunk_limit) is not int or not 256 <= chunk_limit <= cloud_archive.MAX_CHUNK_BYTES:
            raise BundleError('Invalid archive chunk bound.')
        self.core = checkpoint_store
        self.chunk_limit = chunk_limit
        self._temporary = tempfile.TemporaryDirectory(prefix='zoh-private-bundle-')
        self._root = Path(self._temporary.name)
        os.chmod(self._root, 0o700)
        self._cache = None
        self._closed = False

    def close(self):
        if not self._closed:
            self._temporary.cleanup()
            self._cache = None
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _open(self):
        if self._closed:
            raise BundleError('Bundle store is closed.')

    def _put(self, digest, name, raw):
        directory = self._root / digest
        directory.mkdir(mode=0o700, exist_ok=True)
        path = directory / name
        if path.exists():
            if path.is_symlink() or path.read_bytes() != raw:
                raise BundleCorrupt('Verified bundle cache was changed.')
        else:
            with path.open('xb') as stream:
                os.chmod(path, 0o600)
                stream.write(raw)
        return path

    def _read_batches(self, receipt, paths):
        for offset in range(0, len(paths), _READ_BATCH_FILES):
            batch = paths[offset:offset + _READ_BATCH_FILES]
            fetched = self.core.read_files(receipt, batch)
            for path in batch:
                yield path, fetched[path]

    def _download(self, receipt, digests):
        if not digests:
            return
        paths = []
        for path, raw in self._read_batches(receipt, [_prefix(d) + 'manifest.json' for d in digests]):
            digest = path.split('/')[2]
            if len(raw) > cloud_archive.MAX_MANIFEST_BYTES or _hash(raw) != digest:
                raise BundleCorrupt('Remote archive manifest hash does not match.')
            try:
                manifest = json.loads(raw)
                if _json(manifest) != raw or not isinstance(manifest.get('chunks'), list):
                    raise ValueError
                names = [chunk['name'] for chunk in manifest['chunks']]
                if (not names or len(set(names)) != len(names)
                        or any(not isinstance(name, str) or not _CHUNK.fullmatch(name) for name in names)):
                    raise ValueError
            except (ValueError, TypeError, KeyError, AttributeError, UnicodeError, RecursionError):
                raise BundleCorrupt('Remote archive chunk references are invalid.') from None
            self._put(digest, 'manifest.json', raw)
            paths.extend(_prefix(digest) + name for name in names)
        for path, raw in self._read_batches(receipt, paths):
            _, _, digest, name = path.split('/')
            self._put(digest, name, raw)

    def _cache_paths(self):
        result = []
        for digest in self._cache['index']['packages']:
            directory = self._root / digest
            manifest = json.loads((directory / 'manifest.json').read_bytes())
            result.append(_prefix(digest) + 'manifest.json')
            result.extend(_prefix(digest) + chunk['name'] for chunk in manifest['chunks'])
        return result

    def _reuse(self, receipt, index):
        if self._cache is None:
            return []
        old = self._cache['index']['packages']
        if index['packages'][:len(old)] != old:
            raise BundleCorrupt('Remote archive index discarded or reordered verified history.')
        previous = self._cache['receipt']
        if previous.commit == receipt.commit:
            return old
        # Without a tree proof the safe fallback is full remote readback. The
        # optional core extension checks actual old/new Git objects and ancestry.
        proof = getattr(self.core, 'prove_files_unchanged', None)
        if proof is None:
            return []
        if proof(receipt, previous, self._cache_paths()) is not True:
            raise BundleCorrupt('Cached archive Git identities were not confirmed.')
        return old

    def _read(self, checkpoint=None, *, allow_stale_index=False, use_cache=False, expected_index=None):
        self._open()
        loaded = self.core.load() if checkpoint is None else self.core.confirm(checkpoint)
        receipt = loaded.receipt
        try:
            raw = self.core.read_files(receipt, [ARCHIVE_INDEX_PATH])[ARCHIVE_INDEX_PATH]
            index = _index(raw)
            if self._cache is not None:
                history = self._cache['index']['packages']
                if index['packages'][:len(history)] != history:
                    raise BundleCorrupt('Remote archive index discarded or reordered verified history.')
            if expected_index is not None and raw != expected_index:
                raise BundleCorrupt('Remote index differs from the submitted bundle.')
            if not allow_stale_index and index['state_sha256'] != receipt.state_sha256:
                raise BundleCorrupt('Core state is newer than its archive evidence; a new bundle is required.')
            reused = self._reuse(receipt, index) if use_cache else []
            self._download(receipt, index['packages'][len(reused):])
            verified = cloud_archive.verify_packages([self._root / digest for digest in index['packages']])
            if any(verified[key] != index[key] for key in ('manifest_sha256', 'snapshot_sha256', 'generation', 'counts')):
                raise BundleCorrupt('Remote archive does not match its bundle index.')
            # The final check is after local reconstruction, which can be slow.
            self.core.confirm(receipt)
            self._cache = {'receipt': receipt, 'index': index, 'raw': raw}
            bundle = BundleReceipt(receipt.commit, receipt.previous_commit, receipt.state_sha256,
                                   index['manifest_sha256'], index['snapshot_sha256'], _hash(raw), index['generation'])
            return LoadedBundle(bundle, loaded.state, dict(verified['counts']))
        except cloud_archive.ArchivePackageError:
            raise BundleCorrupt('Remote archive verification failed.') from None
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, RecursionError):
            raise BundleCorrupt('Bundle readback failed.') from None

    def load(self):
        """Fresh complete remote readback. Never creates or resets missing state."""
        return self._read()

    def confirm(self, receipt):
        """Confirm the exact current bundle; an advanced head is a conflict."""
        if not isinstance(receipt, BundleReceipt):
            raise BundleError('A verified bundle receipt is required.')
        loaded = self._read(receipt.checkpoint, use_cache=True)
        if loaded.receipt != receipt:
            raise BundleCorrupt('Bundle receipt differs from remote evidence.')
        return loaded

    def bootstrap(self, state, archive_source):
        """Explicit first atomic bundle, only when the branch does not exist."""
        return self._write(state, archive_source, expected_head=None)

    def commit(self, state, archive_source, *, expected_head):
        """Append a verified archive delta and core state in a single Git commit."""
        if not isinstance(expected_head, str) or not re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', expected_head):
            raise BundleError('An explicit existing checkpoint head is required.')
        return self._write(state, archive_source, expected_head=expected_head)

    def _write(self, state, archive_source, *, expected_head):
        self._open()
        # Copy once so caller mutation cannot change the state/index binding.
        state_raw = _json(state)
        if len(state_raw) > MAX_BYTES:
            raise BundleError('Core state exceeds its size bound.')
        state = json.loads(state_raw)
        parents = []
        digests = []
        if expected_head is not None:
            previous = self._read(allow_stale_index=True, use_cache=True)
            if previous.receipt.commit != expected_head:
                raise CheckpointConflict('Bundle head differs from the expected head.')
            digests = list(self._cache['index']['packages'])
            parents = [self._root / digest for digest in digests]
        try:
            with tempfile.TemporaryDirectory(prefix='zoh-bundle-export-', dir=self._root) as scratch:
                package = Path(scratch) / 'package'
                verified = cloud_archive.export_archive(archive_source, package, parents=parents,
                                                       chunk_limit=self.chunk_limit)
                digest = verified['manifest_sha256']
                manifest_raw = (package / 'manifest.json').read_bytes()
                manifest = json.loads(manifest_raw)
                names = ['manifest.json'] + [chunk['name'] for chunk in manifest['chunks']]
                files = {_prefix(digest) + name: (package / name).read_bytes() for name in names}
                index = {'version': 1, 'state_sha256': _hash(state_raw), 'packages': digests + [digest], **verified}
                index_raw = _json(index)
                _index(index_raw)
                files[ARCHIVE_INDEX_PATH] = index_raw
                if expected_head is None:
                    receipt = self.core.bootstrap(state, files=files)
                else:
                    receipt = self.core.commit(state, expected_head=expected_head, files=files)
                try:
                    return self._read(receipt, use_cache=True, expected_index=index_raw).receipt
                except CheckpointError:
                    # The push may have succeeded. Only a later load/confirm can
                    # establish durability; do not automatically push or POST again.
                    raise BundleUncertain(receipt) from None
        except cloud_archive.ArchivePackageError:
            raise BundleCorrupt('Archive export failed; no bundle was submitted.') from None
        except OSError:
            raise BundleError('Local bundle preparation failed.') from None

    def restore(self, target):
        """Restore a verified bundle to a NEW offline SQLite path and return state.

        The core state is returned, never written to a live runtime. The result
        describes this commit's snapshot, not an execution lease after return.
        """
        loaded = self.load()
        try:
            restored = cloud_archive.restore_archive(
                [self._root / digest for digest in self._cache['index']['packages']], target)
            if restored['snapshot_sha256'] != loaded.receipt.archive_snapshot_sha256:
                raise BundleCorrupt('Restored archive differs from the verified bundle.')
        except cloud_archive.ArchivePackageError:
            raise BundleCorrupt('Bundle archive restoration failed.') from None
        return loaded
