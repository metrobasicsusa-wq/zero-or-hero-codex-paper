"""Offline logical archive packages, with no remote upload or trading integration.

Packages hold private evidence, not sanitized public artifacts. Callers supply
explicit database/package paths. Export reads one SQLite transaction; restore
verifies a complete baseline/incremental chain before creating a NEW database.
It never replaces an existing database or resumes a trader. This prototype uses
memory proportional to a logical snapshot; bounded files do not imply bounded
memory, authentication, remote durability, or unlimited free storage.
"""
from __future__ import annotations

import base64
from contextlib import closing
from functools import lru_cache
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import zlib


FORMAT = 'zoh-offline-archive-v1'
MAX_CHUNK_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_BYTES = 4 * 1024 * 1024
TABLES = {
    'blobs': ('sha256', 'kind', 'created_at', 'encoding', 'raw_bytes', 'compressed_bytes', 'payload'),
    'runs': ('run_id', 'action', 'started_at', 'finished_at', 'status', 'config_hash',
             'strategy_hash', 'source_hash', 'snapshot_hash', 'error_json'),
    'events': ('sequence', 'event_id', 'run_id', 'at', 'kind', 'payload_hash'),
    'samples': ('sequence', 'sample_id', 'run_id', 'at', 'symbol', 'sleeve', 'decision', 'sample_hash'),
    'datasets': ('sequence', 'dataset_id', 'run_id', 'at', 'name', 'blob_hash', 'metadata_json'),
}
_HASH = re.compile(r'[0-9a-f]{64}\Z')


class ArchivePackageError(RuntimeError):
    """Fixed diagnostics deliberately exclude archived values and private paths."""


def _fail(message):
    raise ArchivePackageError(message) from None


def _json(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeError, RecursionError):
        _fail('Archive package contains unsupported JSON values.')


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _parse(raw):
    try:
        value = json.loads(raw)
        if _json(value) != raw:
            _fail('Archive JSON is not canonical finite UTF-8.')
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError):
        _fail('Archive JSON cannot be decoded.')


def _cell(value):
    if isinstance(value, bytes):
        return {'$blob': base64.b64encode(value).decode('ascii')}
    if value is None or isinstance(value, (str, int)):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    _fail('Unsupported SQLite cell type.')


def _uncell(value):
    if isinstance(value, dict) and set(value) == {'$blob'}:
        try:
            raw = base64.b64decode(value['$blob'], validate=True)
            if base64.b64encode(raw).decode('ascii') != value['$blob']:
                raise ValueError()
            return raw
        except (ValueError, TypeError):
            _fail('Invalid binary cell encoding.')
    if value is None or isinstance(value, (str, int)) and not isinstance(value, bool):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    _fail('Unsupported encoded SQLite cell.')


def _schema(db):
    return [list(row) for row in db.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name!='sqlite_sequence' ORDER BY type,name")]


@lru_cache(maxsize=1)
def _reviewed_schema():
    """Derive exact version-1 DDL from the reviewed application implementation.

    The empty fixture is disposable and never calls data_dir/start_run, reads
    runtime state, or captures any application sources or credentials.
    """
    from .archive import ResearchArchive
    with tempfile.TemporaryDirectory(prefix='zoh-empty-archive-schema-') as temporary:
        empty = ResearchArchive(temporary)
        with closing(sqlite3.connect(empty.path)) as db:
            return tuple(tuple(row) for row in _schema(db))


def _source_snapshot(path):
    path = Path(path)
    if not path.is_file():
        _fail('Source archive must already exist.')
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=30)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            schema = _schema(db)
            rows = {}
            for table, columns in TABLES.items():
                actual = tuple(row[1] for row in db.execute('PRAGMA table_info(' + table + ')'))
                if actual != columns:
                    _fail('Source archive has an unsupported table layout.')
                rows[table] = [[_cell(cell) for cell in row] for row in db.execute(
                    'SELECT * FROM ' + table + ' ORDER BY ' + columns[0])]
            sequences = [list(row) for row in db.execute('SELECT name,seq FROM sqlite_sequence ORDER BY name')]
            result = {'schema': schema, 'tables': rows, 'sequences': sequences,
                      'user_version': db.execute('PRAGMA user_version').fetchone()[0],
                      'application_id': db.execute('PRAGMA application_id').fetchone()[0]}
            if db.execute('PRAGMA integrity_check').fetchall() != [('ok',)] or db.execute('PRAGMA foreign_key_check').fetchall():
                _fail('Source SQLite integrity checks failed.')
            db.rollback()
        _validate(result)
        return result
    except (OSError, sqlite3.Error):
        raise ArchivePackageError('Source archive snapshot failed.') from None


def _gunzip(raw, expected):
    if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
        _fail('Invalid decompressed length.')
    try:
        decoder = zlib.decompressobj(31)
        result = decoder.decompress(raw, expected + 1)
        if len(result) != expected or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
            _fail('Compressed content length or framing is invalid.')
        return result
    except zlib.error:
        raise ArchivePackageError('Compressed content is damaged.') from None


def _index(rows):
    result = {}
    for row in rows:
        if row[0] in result:
            _fail('Archive contains duplicate row identities.')
        result[row[0]] = row
    return result


def _validate(model):
    if not isinstance(model, dict) or set(model) != {'schema', 'tables', 'sequences', 'user_version', 'application_id'}:
        _fail('Unsupported logical snapshot layout.')
    schema = model['schema']
    if (not isinstance(schema, list) or any(not isinstance(row, list) or len(row) != 4 for row in schema)
            or {row[1] for row in schema if row[0] == 'table'} != set(TABLES)
            or any(row[0] not in {'table', 'index', 'trigger'} or row[2] not in TABLES for row in schema)
            or any(not isinstance(row[1], str) or not isinstance(row[2], str)
                   or (row[3] is not None and not isinstance(row[3], str)) for row in schema)):
        _fail('Unsupported archive schema.')
    if schema != sorted(schema, key=lambda row: (row[0], row[1])) or len({(r[0], r[1]) for r in schema}) != len(schema):
        _fail('Archive schema identities or order are invalid.')
    if model['user_version'] != 1 or isinstance(model['user_version'], bool):
        _fail('Unknown archive schema version requires separate review.')
    if tuple(tuple(row) for row in schema) != _reviewed_schema():
        _fail('Archive schema differs from the reviewed immutable version-1 schema.')
    for name in ('user_version', 'application_id'):
        if not isinstance(model[name], int) or isinstance(model[name], bool) or not 0 <= model[name] <= 2147483647:
            _fail('Unsupported SQLite metadata.')
    if not isinstance(model['tables'], dict) or set(model['tables']) != set(TABLES):
        _fail('Archive table set is incomplete.')
    indexed = {}
    for table, columns in TABLES.items():
        rows = model['tables'][table]
        if not isinstance(rows, list):
            _fail('Invalid archive row collection.')
        for row in rows:
            if not isinstance(row, list) or len(row) != len(columns):
                _fail('Invalid archive row layout.')
            for value in row:
                _uncell(value)
            key = row[0]
            if table in ('blobs', 'runs'):
                if not isinstance(key, str) or not key:
                    _fail('Invalid archive row identity.')
            elif not isinstance(key, int) or isinstance(key, bool) or key <= 0:
                _fail('Invalid archive sequence.')
        if rows != sorted(rows, key=lambda row: row[0]):
            _fail('Archive rows are not in stable order.')
        indexed[table] = _index(rows)
        if table in ('events', 'samples', 'datasets'):
            ids = [row[1] for row in rows]
            if any(not isinstance(value, str) or not value for value in ids) or len(set(ids)) != len(ids):
                _fail('Duplicate or invalid stable record IDs.')
    blobs = indexed['blobs']
    decoded = {}
    for digest, row in blobs.items():
        if not _HASH.fullmatch(digest) or row[3] != 'json+gzip':
            _fail('Invalid archive blob hash or encoding.')
        binary = _uncell(row[6])
        if not isinstance(binary, bytes) or row[5] != len(binary):
            _fail('Archive compressed blob length is invalid.')
        raw = _gunzip(binary, row[4])
        if _hash(raw) != digest:
            _fail('Archive blob SHA-256 does not match.')
        decoded[digest] = _parse(raw)
    runs = indexed['runs']
    for row in runs.values():
        if any(row[index] not in blobs for index in (5, 6, 7)) or (row[8] is not None and row[8] not in blobs):
            _fail('Run has a dangling blob reference.')
        manifest = decoded[row[7]]
        if not isinstance(manifest, dict) or not isinstance(manifest.get('files'), list):
            _fail('Source manifest is invalid.')
        for item in manifest['files']:
            if not isinstance(item, dict) or item.get('blob_hash') not in blobs:
                _fail('Source manifest has a dangling nested blob reference.')
            if item.get('bytes') != blobs[item['blob_hash']][4]:
                _fail('Source manifest byte count does not match.')
    for table in ('events', 'samples', 'datasets'):
        blob_column = TABLES[table].index({'events': 'payload_hash', 'samples': 'sample_hash', 'datasets': 'blob_hash'}[table])
        for row in indexed[table].values():
            if row[2] not in runs or row[blob_column] not in blobs:
                _fail('Archive contains dangling run or blob references.')
    sequences = model['sequences']
    if not isinstance(sequences, list) or any(not isinstance(row, list) or len(row) != 2 for row in sequences):
        _fail('Invalid SQLite sequence metadata.')
    if sequences != sorted(sequences, key=lambda row: row[0]) or len({row[0] for row in sequences}) != len(sequences):
        _fail('Duplicate or unsorted SQLite sequence metadata.')
    for name, value in sequences:
        if name not in ('events', 'samples', 'datasets') or not isinstance(value, int) or isinstance(value, bool) or value < max(indexed[name], default=0):
            _fail('SQLite sequence metadata would reuse an existing sequence.')
    if any(indexed[name] and name not in dict(sequences) for name in ('events', 'samples', 'datasets')):
        _fail('Missing SQLite sequence metadata.')


def _run_update(old, new):
    # Only finish_run's final fields may change; historical final rows are frozen.
    if old[3] is not None or old[4] != 'running' or new[3] is None or new[4] == 'running':
        _fail('Incremental package rewrites a finalized or invalid run.')
    if any(old[index] != new[index] for index in (0, 1, 2, 5, 6, 7)):
        _fail('Incremental package rewrites run identity or original versions.')


def _delta(previous, current):
    if any(previous[key] != current[key] for key in ('schema', 'user_version', 'application_id')):
        _fail('Schema changes require a separately reviewed new baseline.')
    changed, before = {}, {}
    for table in TABLES:
        old, new = _index(previous['tables'][table]), _index(current['tables'][table])
        old_max = max(old, default=0) if table in ('events', 'samples', 'datasets') else None
        if old.keys() - new.keys():
            _fail('Incremental export would discard existing evidence.')
        changed[table] = []
        for key, row in new.items():
            if key not in old:
                if old_max is not None and key <= old_max:
                    _fail('Incremental sequence is not append-only.')
                changed[table].append(row)
            elif row != old[key]:
                if table != 'runs':
                    _fail('Incremental export rewrites immutable evidence.')
                _run_update(old[key], row)
                before[key] = _hash(_json(old[key]))
                changed[table].append(row)
    old_seq, new_seq = dict(previous['sequences']), dict(current['sequences'])
    if any(new_seq.get(name, -1) < value for name, value in old_seq.items()):
        _fail('Incremental SQLite sequence metadata moves backwards.')
    return {'rows': changed, 'run_before': before, 'sequences': current['sequences']}


def _counts(model):
    return {table: len(model['tables'][table]) for table in TABLES}


def _read_file(path, limit):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        _fail('Package file is missing, oversized, or a symbolic link.')
    raw = path.read_bytes()
    if len(raw) > limit:
        _fail('Package file exceeds its size bound.')
    return raw


def _read_package(directory):
    directory = Path(directory)
    raw = _read_file(directory / 'manifest.json', MAX_MANIFEST_BYTES)
    manifest = _parse(raw)
    expected_fields = {'format', 'kind', 'generation', 'parent', 'schema', 'user_version', 'application_id',
                       'snapshot_sha256', 'payload_sha256', 'counts', 'chunks', 'chunk_limit'}
    if not isinstance(manifest, dict) or set(manifest) != expected_fields or manifest['format'] != FORMAT:
        _fail('Unsupported package manifest format.')
    limit = manifest['chunk_limit']
    if not isinstance(limit, int) or isinstance(limit, bool) or not 256 <= limit <= MAX_CHUNK_BYTES:
        _fail('Package chunk limit is invalid.')
    chunks = manifest['chunks']
    if not isinstance(chunks, list) or not chunks:
        _fail('Package chunks are missing.')
    parts = []
    for index, chunk in enumerate(chunks):
        if (not isinstance(chunk, dict) or set(chunk) != {'name', 'sha256', 'bytes', 'raw_bytes'}
                or not isinstance(chunk['sha256'], str) or not _HASH.fullmatch(chunk['sha256'])
                or chunk['name'] != 'chunk-' + str(index).zfill(6) + '-' + chunk['sha256'] + '.gz'
                or not isinstance(chunk['raw_bytes'], int) or not 0 <= chunk['raw_bytes'] <= limit):
            _fail('Package chunk descriptor is invalid.')
        packed = _read_file(directory / chunk['name'], limit)
        if len(packed) != chunk['bytes'] or _hash(packed) != chunk['sha256']:
            _fail('Package chunk hash or size does not match.')
        parts.append(_gunzip(packed, chunk['raw_bytes']))
    payload_raw = b''.join(parts)
    if _hash(payload_raw) != manifest['payload_sha256']:
        _fail('Package payload SHA-256 does not match.')
    payload = _parse(payload_raw)
    if not isinstance(payload, dict) or set(payload) != {'rows', 'run_before', 'sequences'}:
        _fail('Package payload layout is invalid.')
    return manifest, payload, _hash(raw)


def _chain(packages):
    if not packages:
        _fail('A complete baseline package chain is required.')
    model, last_hash, last_manifest = None, None, None
    for generation, path in enumerate(packages):
        manifest, payload, digest = _read_package(path)
        if (manifest['generation'] != generation or manifest['parent'] != last_hash
                or manifest['kind'] != ('baseline' if generation == 0 else 'incremental')):
            _fail('Archive package chain is missing, reordered, or has the wrong parent.')
        if not isinstance(payload['rows'], dict) or set(payload['rows']) != set(TABLES) or not isinstance(payload['run_before'], dict):
            _fail('Package table set or run-update preconditions are invalid.')
        if model is None:
            if payload['run_before']:
                _fail('Baseline cannot contain run-update preconditions.')
            model = {'schema': manifest['schema'], 'tables': payload['rows'], 'sequences': payload['sequences'],
                     'user_version': manifest['user_version'], 'application_id': manifest['application_id']}
        else:
            if any(manifest[key] != model[key] for key in ('schema', 'user_version', 'application_id')):
                _fail('Incremental schema or database metadata changed.')
            merged = {}
            applied = set()
            for table in TABLES:
                old = _index(model['tables'][table])
                last_sequence = max(old, default=0) if table in ('events', 'samples', 'datasets') else None
                additions = payload['rows'][table]
                if not isinstance(additions, list):
                    _fail('Invalid incremental rows.')
                seen = set()
                for row in additions:
                    if not isinstance(row, list) or len(row) != len(TABLES[table]) or not isinstance(row[0], (str, int)):
                        _fail('Invalid incremental row layout.')
                    key = row[0]
                    if key in seen:
                        _fail('Duplicate row identity inside an incremental package.')
                    seen.add(key)
                    if key in old:
                        if table != 'runs' or payload['run_before'].get(key) != _hash(_json(old[key])):
                            _fail('Incremental row conflicts with already archived evidence.')
                        _run_update(old[key], row)
                        applied.add(key)
                    elif last_sequence is not None:
                        if not isinstance(key, int) or isinstance(key, bool) or key <= last_sequence:
                            _fail('Incremental sequence is not append-only.')
                        last_sequence = key
                    old[key] = row
                merged[table] = sorted(old.values(), key=lambda row: row[0])
            if applied != set(payload['run_before']):
                _fail('Incremental run-update preconditions are incomplete.')
            prior = model
            model = dict(model, tables=merged, sequences=payload['sequences'])
            _validate(model)
            _delta(prior, model)
        _validate(model)
        if _counts(model) != manifest['counts'] or _hash(_json(model)) != manifest['snapshot_sha256']:
            _fail('Reconstructed snapshot hash or row counts do not match.')
        last_hash, last_manifest = digest, manifest
    return model, last_hash, last_manifest


def verify_packages(packages):
    """Verify an explicitly ordered local chain; hashes are not authentication."""
    try:
        model, digest, manifest = _chain(list(packages))
        return {'manifest_sha256': digest, 'snapshot_sha256': manifest['snapshot_sha256'],
                'generation': manifest['generation'], 'counts': _counts(model)}
    except ArchivePackageError:
        raise
    except (OSError, sqlite3.Error, TypeError, ValueError, KeyError, IndexError, OverflowError):
        raise ArchivePackageError('Archive package verification failed.') from None


def export_archive(source, destination, *, parents=(), chunk_limit=MAX_CHUNK_BYTES):
    """Write a new immutable local package; manifest is written LAST.

    A failed disk write can leave an incomplete destination without a manifest;
    it is never accepted as a package or overwritten on retry. Use a new path.
    Incrementals require the full ordered parent chain, not merely a timestamp.
    """
    if not isinstance(chunk_limit, int) or isinstance(chunk_limit, bool) or not 256 <= chunk_limit <= MAX_CHUNK_BYTES:
        _fail('Chunk limit must be between 256 bytes and 8 MiB.')
    destination = Path(destination)
    if destination.exists():
        _fail('Package destination already exists; immutable packages are never overwritten.')
    try:
        current = _source_snapshot(source)
        parents = list(parents)
        if parents:
            previous, parent, parent_manifest = _chain(parents)
            payload = _delta(previous, current)
            generation, kind = parent_manifest['generation'] + 1, 'incremental'
        else:
            payload = {'rows': current['tables'], 'run_before': {}, 'sequences': current['sequences']}
            generation, parent, kind = 0, None, 'baseline'
        raw = _json(payload)
        # Conservative allowance for incompressible deflate framing. The actual
        # compressed size is checked again before ANY package is published.
        raw_limit = chunk_limit - chunk_limit // 1000 - 128
        packed = [gzip.compress(raw[offset:offset + raw_limit], compresslevel=6, mtime=0)
                  for offset in range(0, len(raw), raw_limit)]
        chunks = []
        for index, chunk in enumerate(packed):
            if len(chunk) > chunk_limit:
                _fail('Compressed archive chunk exceeds its bound.')
            digest = _hash(chunk)
            chunks.append({'name': 'chunk-' + str(index).zfill(6) + '-' + digest + '.gz',
                           'sha256': digest, 'bytes': len(chunk),
                           'raw_bytes': len(raw[index * raw_limit:(index + 1) * raw_limit])})
        manifest = {'format': FORMAT, 'kind': kind, 'generation': generation, 'parent': parent,
                    'schema': current['schema'], 'user_version': current['user_version'],
                    'application_id': current['application_id'], 'snapshot_sha256': _hash(_json(current)),
                    'payload_sha256': _hash(raw), 'counts': _counts(current),
                    'chunks': chunks, 'chunk_limit': chunk_limit}
        manifest_raw = _json(manifest)
        if len(manifest_raw) > MAX_MANIFEST_BYTES:
            _fail('Archive manifest exceeds its bound.')
        destination.mkdir(mode=0o700, parents=False, exist_ok=False)
        for descriptor, chunk in zip(chunks, packed):
            _write_new(destination / descriptor['name'], chunk)
        _write_new(destination / 'manifest.json', manifest_raw)
        # Exact local readback, not a claim of remote upload or remote durability.
        return verify_packages(parents + [destination])
    except ArchivePackageError:
        raise
    except (OSError, sqlite3.Error, TypeError, ValueError, KeyError, IndexError, OverflowError):
        raise ArchivePackageError('Archive package export failed; completion was not confirmed.') from None


def _write_new(path, raw):
    with path.open('xb') as stream:
        os.chmod(path, 0o600)
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _ddl_guard(action, first, second, database, trigger):
    # Package SQL is data, not permission to attach/read external databases or
    # execute extension/functions. DDL is always one statement at a time.
    if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_PRAGMA):
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def restore_archive(packages, target):
    """Rebuild baseline plus incrementals at a NEW offline path, never in place.

    Repeating restoration to a different new path is deterministic logically.
    Existing targets and SQLite sidecars are refused. No live database handoff,
    state cursor update, broker access, or deletion of evidence is performed.
    """
    target = Path(target)
    if any(Path(str(target) + suffix).exists() for suffix in ('', '-wal', '-shm', '-journal')):
        _fail('Restore target or SQLite sidecars already exist; use a new offline path.')
    temporary = None
    try:
        model, digest, manifest = _chain(list(packages))
        if not target.parent.is_dir():
            _fail('Restore target parent directory must already exist.')
        handle, temporary = tempfile.mkstemp(prefix='.archive-restore-', suffix='.sqlite3', dir=target.parent)
        os.close(handle)
        with closing(sqlite3.connect(temporary)) as db:
            db.enable_load_extension(False)
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA synchronous=FULL')
            with db:
                db.set_authorizer(_ddl_guard)
                for kind, name, table, sql in model['schema']:
                    if kind == 'table':
                        db.execute(sql)
                db.set_authorizer(None)
                for table, columns in TABLES.items():
                    db.executemany('INSERT INTO ' + table + ' VALUES (' + ','.join('?' for _ in columns) + ')',
                                   [tuple(_uncell(value) for value in row) for row in model['tables'][table]])
                db.execute('DELETE FROM sqlite_sequence')
                db.executemany('INSERT INTO sqlite_sequence(name,seq) VALUES (?,?)', model['sequences'])
                db.set_authorizer(_ddl_guard)
                for kind, name, table, sql in model['schema']:
                    if kind != 'table' and sql is not None:
                        db.execute(sql)
                db.set_authorizer(None)
                db.execute('PRAGMA user_version=' + str(model['user_version']))
                db.execute('PRAGMA application_id=' + str(model['application_id']))
        rebuilt = _source_snapshot(temporary)
        if _hash(_json(rebuilt)) != manifest['snapshot_sha256']:
            _fail('Restored database failed exact logical readback.')
        # Hard-link installation is atomic and fails if target appeared meanwhile;
        # unlike replace/rename it can never overwrite existing evidence.
        os.link(temporary, target)
        os.unlink(temporary)
        temporary = None
        return {'manifest_sha256': digest, 'snapshot_sha256': manifest['snapshot_sha256'],
                'generation': manifest['generation'], 'counts': _counts(rebuilt)}
    except ArchivePackageError:
        raise
    except (OSError, sqlite3.Error, TypeError, ValueError, KeyError, IndexError, OverflowError):
        raise ArchivePackageError('Archive restoration failed; target completion was not confirmed.') from None
    finally:
        if temporary is not None:
            # Only remove the uniquely created scratch database, never the target.
            for suffix in ('', '-wal', '-shm', '-journal'):
                scratch = Path(temporary + suffix)
                if scratch.exists():
                    scratch.unlink()
