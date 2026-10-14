"""Durable, append-only paper-trading evidence for later research.

Only explicitly supplied JSON and allowlisted application source are archived.
Credentials and private runtime files are never enumerated. Blobs are canonical
JSON, content addressed, compressed, and verified when read back.
"""
from contextlib import contextmanager
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import uuid

from .storage import ROOT


REDACTED = '[REDACTED]'
_SECRET_FIELDS = {'authorization', 'proxyauthorization', 'password', 'passwd',
                  'credential', 'credentials', 'apikey', 'apisecret', 'secret',
                  'secretkey', 'token', 'accesstoken', 'refreshtoken', 'idtoken',
                  'cookie', 'setcookie', 'privatekey'}
_CREDENTIAL_TEXT = re.compile(
    r'(?i)(\b(?:authorization|proxy-authorization)\s*[:=]\s*)(?:bearer\s+|basic\s+)?[^\s,;]+|'
    r'(\b(?:APCA[-_]API[-_](?:KEY[-_]ID|SECRET[-_]KEY)|API[-_]KEY|API[-_]SECRET|'
    r'ACCESS[-_]TOKEN|REFRESH[-_]TOKEN|PASSWORD)\s*[:=]\s*)[^\s,;]+')


def _is_secret(name):
    key = re.sub(r'[^a-z0-9]', '', name.lower())
    return key in _SECRET_FIELDS or 'apikey' in key or 'apisecret' in key or key.endswith('password') or key.endswith('secret') or key.endswith('secretkey')


def redact(value):
    """Remove common credential fields without mutating the caller's objects."""
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise TypeError('Archive JSON object keys must be strings.')
        return {key: REDACTED if _is_secret(key) else redact(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return _CREDENTIAL_TEXT.sub(lambda match: (match.group(1) or match.group(2)) + REDACTED, value)
    return value


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _prepared(value):
    raw = _json(redact(value)).encode('utf-8')
    return hashlib.sha256(raw).hexdigest(), raw


def _at(value=None):
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError('Archive timestamps must contain a timezone.')
        return value.astimezone(timezone.utc).isoformat()
    if not isinstance(value, str) or not value:
        raise ValueError('Archive timestamp must be a nonempty ISO timestamp.')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Archive timestamps must contain a timezone.')
    return parsed.astimezone(timezone.utc).isoformat()


class ResearchArchive:
    """SQLite archive below a private runtime directory; never prunes evidence."""

    def __init__(self, directory):
        self.directory = Path(directory) / 'archive'
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / 'research.sqlite3'
        with self._connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS blobs (
                    sha256 TEXT PRIMARY KEY, kind TEXT NOT NULL, created_at TEXT NOT NULL,
                    encoding TEXT NOT NULL, raw_bytes INTEGER NOT NULL,
                    compressed_bytes INTEGER NOT NULL, payload BLOB NOT NULL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY, action TEXT NOT NULL, started_at TEXT NOT NULL,
                    finished_at TEXT, status TEXT NOT NULL,
                    config_hash TEXT NOT NULL REFERENCES blobs(sha256),
                    strategy_hash TEXT NOT NULL REFERENCES blobs(sha256),
                    source_hash TEXT NOT NULL REFERENCES blobs(sha256),
                    snapshot_hash TEXT REFERENCES blobs(sha256), error_json TEXT
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL REFERENCES runs(run_id), at TEXT NOT NULL,
                    kind TEXT NOT NULL, payload_hash TEXT NOT NULL REFERENCES blobs(sha256)
                );
                CREATE TABLE IF NOT EXISTS samples (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, sample_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL REFERENCES runs(run_id), at TEXT NOT NULL,
                    symbol TEXT, sleeve TEXT, decision TEXT,
                    sample_hash TEXT NOT NULL REFERENCES blobs(sha256)
                );
                CREATE TABLE IF NOT EXISTS datasets (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, dataset_id TEXT NOT NULL UNIQUE,
                    run_id TEXT NOT NULL REFERENCES runs(run_id), at TEXT NOT NULL,
                    name TEXT NOT NULL, blob_hash TEXT NOT NULL REFERENCES blobs(sha256),
                    metadata_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS events_run ON events(run_id, sequence);
                CREATE INDEX IF NOT EXISTS samples_run ON samples(run_id, sequence);
                CREATE INDEX IF NOT EXISTS samples_symbol ON samples(symbol, at);
                CREATE INDEX IF NOT EXISTS datasets_run ON datasets(run_id, sequence);
                PRAGMA user_version = 1;
            ''')
            for table in ('blobs', 'events', 'samples', 'datasets'):
                for operation in ('UPDATE', 'DELETE'):
                    db.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_{table}_{operation.lower()}
                        BEFORE {operation} ON {table} BEGIN
                        SELECT RAISE(ABORT, 'Research archive records are immutable'); END''')
            db.execute('''CREATE TRIGGER IF NOT EXISTS immutable_runs_delete
                BEFORE DELETE ON runs BEGIN SELECT RAISE(ABORT, 'Research runs cannot be deleted'); END''')

    def close(self):
        """Connections are already closed after every operation."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA busy_timeout=30000')
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _put(db, prepared, kind, at):
        digest, raw = prepared
        if db.execute('SELECT 1 FROM blobs WHERE sha256=?', (digest,)).fetchone() is None:
            compressed = gzip.compress(raw, compresslevel=6, mtime=0)
            db.execute('''INSERT INTO blobs
                (sha256,kind,created_at,encoding,raw_bytes,compressed_bytes,payload)
                VALUES (?,?,?,'json+gzip',?,?,?)''',
                (digest, kind, at, len(raw), len(compressed), compressed))
        return digest

    def put_blob(self, value, kind):
        prepared = _prepared(value)
        with self._connect() as db:
            return self._put(db, prepared, str(kind), _at())

    @staticmethod
    def _get(db, digest):
        row = db.execute('SELECT encoding,payload,raw_bytes FROM blobs WHERE sha256=?', (digest,)).fetchone()
        if row is None:
            raise KeyError(f'Archive blob not found: {digest}')
        if row['encoding'] != 'json+gzip':
            raise ValueError('Unsupported archive blob encoding.')
        try:
            raw = gzip.decompress(row['payload'])
        except (OSError, EOFError) as exc:
            raise ValueError('Archive blob failed decompression.') from exc
        if len(raw) != row['raw_bytes'] or hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError('Archive blob content hash does not match.')
        return json.loads(raw)

    def get_blob(self, digest):
        with self._connect() as db:
            return self._get(db, digest)

    def _sources(self, db, at):
        # Fixed application paths, never runtime state, credentials, environment,
        # arbitrary user files, or recursive directory traversal.
        root = Path(ROOT).resolve()
        paths = list((root / 'zerotrader').glob('*.py'))
        paths.extend(root / name for name in ('run_trader.py', 'trader_worker.py',
                     'Run-Trader.ps1', 'Manage-Trader.ps1', 'Manage-TraderStartup.ps1', 'AlpacaPaper.psm1', 'settings.json'))
        manifest = {'format': 1, 'python': sys.version, 'files': []}
        for path in sorted(paths):
            if not path.is_file():
                continue
            if not path.resolve().is_relative_to(root):
                raise ValueError('Source snapshot path escapes the application directory.')
            content = path.read_bytes().decode('utf-8-sig')
            value = json.loads(content) if path.name == 'settings.json' else content
            prepared = _prepared(value)
            digest = self._put(db, prepared, 'source', at)
            manifest['files'].append({'path': path.relative_to(root).as_posix(),
                                      'blob_hash': digest, 'bytes': len(prepared[1])})
        return self._put(db, _prepared(manifest), 'source_manifest', at)

    def start_run(self, action, config, strategy_state, at=None):
        at, run_id = _at(at), uuid.uuid4().hex
        config_data, strategy_data = _prepared(config), _prepared(strategy_state)
        with self._connect() as db:
            config_hash = self._put(db, config_data, 'config', at)
            strategy_hash = self._put(db, strategy_data, 'strategy_state', at)
            source_hash = self._sources(db, at)
            db.execute('''INSERT INTO runs
                (run_id,action,started_at,status,config_hash,strategy_hash,source_hash)
                VALUES (?,?,?,'running',?,?,?)''',
                (run_id, str(action), at, config_hash, strategy_hash, source_hash))
            self._event(db, run_id, 'run_started', _prepared({'action': action}), at)
        return run_id

    @staticmethod
    def _event(db, run_id, kind, prepared, at):
        event_id = uuid.uuid4().hex
        digest = ResearchArchive._put(db, prepared, 'event', at)
        db.execute('INSERT INTO events(event_id,run_id,at,kind,payload_hash) VALUES (?,?,?,?,?)',
                   (event_id, run_id, at, str(kind), digest))
        return event_id

    def event(self, run_id, kind, payload, at=None):
        prepared, at = _prepared(payload), _at(at)
        with self._connect() as db:
            return self._event(db, run_id, kind, prepared, at)

    def event_once(self, run_id, kind, payload, key, at=None):
        """Record a stable broker event once across retries and later runs.

        The caller supplies a non-secret stable key (e.g. broker fill ID) and
        invariant payload; reusing a key for different evidence is an error.
        """
        if not isinstance(key, str) or not key:
            raise ValueError('An idempotent archive event needs a nonempty key.')
        prepared, at = _prepared(payload), _at(at)
        event_id = 'once-' + hashlib.sha256(key.encode('utf-8')).hexdigest()
        with self._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = db.execute('SELECT kind,payload_hash FROM events WHERE event_id=?', (event_id,)).fetchone()
            if existing is not None:
                if existing['kind'] != str(kind) or existing['payload_hash'] != prepared[0]:
                    raise ValueError('An existing archive event key has different evidence.')
                return event_id
            digest = self._put(db, prepared, 'event', at)
            db.execute('INSERT INTO events(event_id,run_id,at,kind,payload_hash) VALUES (?,?,?,?,?)',
                       (event_id, run_id, at, str(kind), digest))
        return event_id

    def samples(self, run_id, rows):
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise TypeError('Research samples must be a list of JSON objects.')
        at = _at()
        # Validate every row before writing, so a bad late sample never leaves a
        # partly archived selection batch.
        prepared_rows = [(_prepared(row), _at(row.get('at', row.get('asof', at))), row) for row in rows]
        with self._connect() as db:
            for prepared, observed_at, row in prepared_rows:
                digest = self._put(db, prepared, 'sample', at)
                scalar = lambda value: value if value is None or isinstance(value, str) else _json(redact(value))
                db.execute('''INSERT INTO samples
                    (sample_id,run_id,at,symbol,sleeve,decision,sample_hash) VALUES (?,?,?,?,?,?,?)''',
                    (uuid.uuid4().hex, run_id, observed_at, scalar(row.get('symbol')),
                     scalar(row.get('sleeve')), scalar(row.get('decision')), digest))

    def dataset(self, run_id, name, data, metadata=None):
        prepared, metadata_json, at = _prepared(data), _json(redact(metadata or {})), _at()
        with self._connect() as db:
            digest = self._put(db, prepared, 'dataset', at)
            db.execute('''INSERT INTO datasets(dataset_id,run_id,at,name,blob_hash,metadata_json)
                VALUES (?,?,?,?,?,?)''', (uuid.uuid4().hex, run_id, at, str(name), digest, metadata_json))
        return digest

    def finish_run(self, run_id, status, snapshot=None, error=None):
        at = _at()
        snapshot_data = _prepared(snapshot) if snapshot is not None else None
        error_json = _json(redact(error)) if error is not None else None
        status = str(status)
        if not status or status == 'running':
            raise ValueError('A finished run needs a final status.')
        with self._connect() as db:
            row = db.execute('SELECT finished_at FROM runs WHERE run_id=?', (run_id,)).fetchone()
            if row is None:
                raise KeyError(f'Archive run not found: {run_id}')
            if row['finished_at'] is not None:
                raise ValueError('Archive run is already finished.')
            snapshot_hash = self._put(db, snapshot_data, 'snapshot', at) if snapshot_data is not None else None
            self._event(db, run_id, 'run_finished', _prepared({'status': status,
                        'snapshot_hash': snapshot_hash, 'error': error}), at)
            db.execute('UPDATE runs SET finished_at=?,status=?,snapshot_hash=?,error_json=? WHERE run_id=?',
                       (at, status, snapshot_hash, error_json, run_id))

    def stats(self):
        with self._connect() as db:
            counts = {table: db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                      for table in ('runs', 'events', 'samples', 'datasets', 'blobs')}
            sizes = db.execute('SELECT COALESCE(SUM(raw_bytes),0),COALESCE(SUM(compressed_bytes),0) FROM blobs').fetchone()
        counts.update({'raw_blob_bytes': sizes[0], 'compressed_blob_bytes': sizes[1],
                       'database_bytes': self.path.stat().st_size,
                       'wal_bytes': Path(str(self.path) + '-wal').stat().st_size if Path(str(self.path) + '-wal').exists() else 0,
                       'path': str(self.path)})
        return counts

    def export_csv(self, output_dir):
        """Export a consistent read snapshot; each JSON column stays one cell."""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        specs = {
            'runs': ('SELECT * FROM runs ORDER BY started_at,run_id',
                     ('config_hash', 'strategy_hash', 'source_hash', 'snapshot_hash')),
            'events': ('SELECT * FROM events ORDER BY sequence', ('payload_hash',)),
            'samples': ('SELECT * FROM samples ORDER BY sequence', ('sample_hash',)),
            'datasets': ('SELECT * FROM datasets ORDER BY sequence', ())
        }
        result = {'directory': str(output_dir), 'files': {}, 'counts': {}}
        with self._connect() as db:
            db.execute('BEGIN')
            for table, (query, expand) in specs.items():
                cursor = db.execute(query)
                fields = [column[0] for column in cursor.description]
                json_fields = [field.removesuffix('_hash') + '_json' for field in expand]
                path = output_dir / (table + '.csv')
                count = 0
                with path.open('w', encoding='utf-8-sig', newline='') as stream:
                    writer = csv.DictWriter(stream, fieldnames=fields + json_fields)
                    writer.writeheader()
                    for row in cursor:
                        record = dict(row)
                        for field, json_field in zip(expand, json_fields):
                            record[json_field] = _json(self._get(db, record[field])) if record[field] else ''
                        # Protect spreadsheet users from cells interpreted as
                        # formulas; the exact value remains in JSON/blob storage.
                        record = {key: "'" + value if isinstance(value, str) and value.startswith(('=', '+', '-', '@')) else value
                                  for key, value in record.items()}
                        writer.writerow(record)
                        count += 1
                result['files'][table] = str(path)
                result['counts'][table] = count
        return result
