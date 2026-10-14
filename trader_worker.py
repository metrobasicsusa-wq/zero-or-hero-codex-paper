"""Single-instance local supervisor for PAPER trading cycles.

Run directly only when continuous execution has been explicitly enabled. The
supervisor never reads API credentials and never persists child output verbatim.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from zerotrader.storage import ROOT, data_dir, read_json, write_json, named_lock


CHILD_TIMEOUT_SECONDS = 240
HEARTBEAT_SECONDS = 10
OPEN_INTERVAL_SECONDS = 60
CLOSED_INTERVAL_SECONDS = 300
SAFE_SUMMARY_FIELDS = {
    "at", "mode", "enabled", "paused", "halted", "equity", "cash",
    "drawdown", "market_open", "last_research",
}


def utcnow():
    return datetime.now(timezone.utc)


@contextmanager
def worker_lock(directory):
    """A worker-wide OS lock, intentionally distinct from runner.lock."""
    with named_lock(directory, 'worker.lock'):
        yield


def interval_seconds(directory):
    """Use the last exchange-clock state, defaulting to quicker safety checks."""
    try:
        latest = read_json(Path(directory) / "latest.json", {})
        if latest.get("market_open") is False:
            return CLOSED_INTERVAL_SECONDS
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return OPEN_INTERVAL_SECONDS


def safe_summary(stdout):
    """Allowlist benign scalar report fields; never retain raw errors or output."""
    if len(stdout) > 65536:
        return {"output_available": False}
    try:
        value = json.loads(stdout)
    except (ValueError, TypeError):
        return {"output_available": False}
    if not isinstance(value, dict):
        return {"output_available": False}
    summary = {}
    for key in SAFE_SUMMARY_FIELDS:
        item = value.get(key)
        if key in {"enabled", "paused", "halted", "market_open"}:
            if isinstance(item, bool):
                summary[key] = item
        elif key in {"equity", "cash", "drawdown"}:
            if isinstance(item, (float, int)) and not isinstance(item, bool):
                # Reject non-finite JSON values before atomic strict JSON writing.
                import math
                if math.isfinite(item):
                    summary[key] = item
        elif key == "mode":
            if item == "PAPER":
                summary[key] = item
        elif key in {"at", "last_research"} and isinstance(item, str):
            try:
                parsed = datetime.fromisoformat(item.replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    summary[key] = parsed.isoformat()
            except ValueError:
                pass
    summary["output_available"] = bool(summary)
    return summary


def _heartbeat(directory, state, **changes):
    state.update(changes)
    state["heartbeat_utc"] = utcnow().isoformat()
    write_json(Path(directory) / "worker.json", state)


def run_child(directory, state, timeout_seconds=CHILD_TIMEOUT_SECONDS):
    """Run one cycle, keeping heartbeat fresh and killing only this owned child."""
    started = utcnow().isoformat()
    process = subprocess.Popen(
        [str(Path(sys.executable).with_name("python.exe")) if os.name == "nt" else sys.executable,
         str(ROOT / "run_trader.py"), "cycle"],
        cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", errors="replace",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    try:
        _heartbeat(directory, state, phase="cycle", child_pid=process.pid,
                   last_cycle_started_utc=started, next_cycle_utc=None)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                process.kill()
                stdout, _ = process.communicate(timeout=10)
                break
            try:
                stdout, _ = process.communicate(timeout=min(HEARTBEAT_SECONDS, remaining))
                break
            except subprocess.TimeoutExpired:
                _heartbeat(directory, state, phase="cycle")
    except BaseException:
        # Do not leave an owned cycle executing if the supervisor itself exits.
        if process.poll() is None:
            process.kill()
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            if process.stdout:
                process.stdout.close()
            if process.stderr:
                process.stderr.close()
        raise
    finished = utcnow().isoformat()
    outcome = "timeout" if timed_out else ("ok" if process.returncode == 0 else "error")
    result = {"started_utc": started, "finished_utc": finished,
              "returncode": process.returncode, "timed_out": timed_out,
              "outcome": outcome, "summary": safe_summary(stdout)}
    write_json(Path(directory) / "worker-last-output.json", result)
    _heartbeat(directory, state, child_pid=None, last_cycle_finished_utc=finished,
               last_returncode=process.returncode, last_outcome=outcome,
               consecutive_errors=0 if outcome == "ok" else state.get("consecutive_errors", 0) + 1)
    return result


def run_worker(directory=None):
    directory = Path(directory) if directory is not None else data_dir()
    directory.mkdir(parents=True, exist_ok=True)
    stop_file = directory / "stop-worker.flag"
    with worker_lock(directory):
        state = {"pid": os.getpid(), "script": str(Path(__file__).resolve()),
                 "python": str(Path(sys.executable).resolve()), "mode": "PAPER",
                 "started_utc": utcnow().isoformat(), "phase": "starting",
                 "child_pid": None, "consecutive_errors": 0, "next_cycle_utc": None}
        _heartbeat(directory, state)
        try:
            # Start immediately. A prior graceful-stop flag must be cleared by
            # the explicit Start controller, never silently by this process.
            while not stop_file.exists():
                try:
                    run_child(directory, state)
                except Exception:
                    # Exception text can contain arbitrary child/process data.
                    # Persist a fixed diagnostic instead of exception content.
                    _heartbeat(directory, state, phase="error", child_pid=None,
                               last_outcome="supervisor_error",
                               consecutive_errors=state.get("consecutive_errors", 0) + 1)
                    write_json(directory / "worker-last-output.json", {
                        "finished_utc": utcnow().isoformat(), "outcome": "supervisor_error",
                        "message": "Cycle supervisor failed; next cycle will reconcile broker state."})
                interval = interval_seconds(directory)
                next_at = utcnow() + timedelta(seconds=interval)
                _heartbeat(directory, state, phase="waiting", child_pid=None,
                           interval_seconds=interval, next_cycle_utc=next_at.isoformat())
                deadline = time.monotonic() + interval
                while not stop_file.exists() and time.monotonic() < deadline:
                    time.sleep(min(1, max(0, deadline - time.monotonic())))
                    if (utcnow() - datetime.fromisoformat(state["heartbeat_utc"])).total_seconds() >= HEARTBEAT_SECONDS:
                        _heartbeat(directory, state)
        finally:
            _heartbeat(directory, state, phase="stopped", child_pid=None,
                       stopped_utc=utcnow().isoformat(), next_cycle_utc=None)


def main():
    try:
        run_worker()
    except RuntimeError:
        print(json.dumps({"running": False, "reason": "Worker singleton lock is already held or unavailable."}))
        return 2
    except KeyboardInterrupt:
        return 0
    except Exception:
        print(json.dumps({"running": False, "reason": "Worker could not start; inspect safe worker metadata."}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
