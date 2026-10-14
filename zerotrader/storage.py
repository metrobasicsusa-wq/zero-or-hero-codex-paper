import contextlib
import json
import os
from pathlib import Path
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def data_dir():
    explicit = os.environ.get('ZOH_PRIVATE_RUNTIME')
    if explicit:
        result = Path(explicit)
        if not result.is_absolute() or not result.is_dir():
            raise RuntimeError('Cloud private runtime must be an existing absolute directory.')
        return result
    base = os.environ.get('LOCALAPPDATA')
    if not base:
        raise RuntimeError('Windows LOCALAPPDATA is required for private runtime state.')
    result = Path(base) / 'ZeroOrHero' / 'autotrader'
    result.mkdir(parents=True, exist_ok=True)
    return result


def read_json(path, default=None):
    path = Path(path)
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp = tempfile.mkstemp(prefix=path.name, suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(handle, 'w', encoding='utf-8') as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


@contextlib.contextmanager
def named_lock(directory, name):
    path = Path(directory) / name
    with path.open('a+b') as stream:
        stream.seek(0, 2)
        if stream.tell() == 0:
            stream.write(b'0')
            stream.flush()
        stream.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError('Another trading cycle is running; this cycle was skipped.') from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == 'nt':
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def process_lock(directory):
    return named_lock(directory, 'runner.lock')
