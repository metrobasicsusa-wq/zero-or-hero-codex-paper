"""Opt-in private-state cloud coordinator; never used by the local worker.

The CLI is gated to the reviewed GitHub repository/main workflow. It cannot
bootstrap a new account or clear paused/halted state. A separate native cutover
must stop/disable the local executor and upload a complete private baseline.
Workflow concurrency and all authorized writers preserving non-force history
are required; Git checkpoints alone are not execution leases.
Cold-start archive failure also prevents broker construction because the private
cutover proof is inside that archive. Existing broker stops remain, but active
exits cannot be promised until complete recovery succeeds. Archive-only failures
AFTER verified startup retain confirmed-core protective operations.
"""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import argparse
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import re
import signal
import sqlite3
import tempfile
import time

from . import runner
from .archive import ResearchArchive
from .broker import PaperBroker
from .cloud_auxiliary import AuxiliaryError, capture_auxiliary, restore_auxiliary
from .cloud_bundle import BundleCorrupt, BundleReceipt, GitBundleStore
from .cloud_checkpoint import CheckpointReceipt, LoadedCheckpoint, _canonical, _check_state
from .cloud_engine import CloudDurabilityError, CloudEngine
from .cloud_github import PrivateGithubCheckpointStore, STATE_REPOSITORY, normalize_secret_text
from .data import NY, timestamp
from .risk import validate_config
from .storage import ROOT, process_lock, read_json, write_json


CODE_REPOSITORY = "metrobasicsusa-wq/zero-or-hero-codex-paper"
_HASH = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"[0-9a-f]{40}\Z")
_ASCII_WHITESPACE = " \t\r\n\v\f"
_CUTOVER_FIELDS = frozenset({
    "version", "authority", "code_repository", "state_repository", "branch",
    "account_sha256", "local_worker_stopped", "local_logon_task_disabled",
    "stopped_at", "source_commit", "evidence_cutoff",
})


class CloudRunError(RuntimeError):
    """Fixed, content-free public diagnostic."""


class CloudRunInterrupted(CloudRunError):
    pass


def account_digest(state):
    _check_state(state)
    return hashlib.sha256(state["account_id"].encode("utf-8")).hexdigest()


def validate_cutover(proof, state, expected_account_sha256, *, now=None):
    now = now or datetime.now(timezone.utc)
    try:
        if (not isinstance(expected_account_sha256, str) or not _HASH.fullmatch(expected_account_sha256)
                or not hmac.compare_digest(account_digest(state), expected_account_sha256)
                or not isinstance(proof, dict) or set(proof) != _CUTOVER_FIELDS
                or type(proof["version"]) is not int or proof["version"] != 1
                or proof["authority"] != "github-actions"
                or proof["code_repository"] != CODE_REPOSITORY
                or proof["state_repository"] != STATE_REPOSITORY or proof["branch"] != "main"
                or proof["account_sha256"] != expected_account_sha256
                or proof["local_worker_stopped"] is not True
                or proof["local_logon_task_disabled"] is not True
                or not isinstance(proof["source_commit"], str) or not _OID.fullmatch(proof["source_commit"])
                or type(proof["evidence_cutoff"]) is not int or proof["evidence_cutoff"] <= 0):
            raise ValueError
        stopped = timestamp(proof["stopped_at"])
        if stopped.tzinfo is None or stopped.utcoffset() != timedelta(0) or stopped > now:
            raise ValueError
    except Exception:
        raise CloudRunError("Verified private cutover and matching account binding are required.") from None
    return True


def validate_environment(environ):
    expected = normalize_secret_text(environ.get("ZOH_EXPECTED_ACCOUNT_SHA256", ""))
    if (environ.get("GITHUB_ACTIONS") != "true"
            or environ.get("GITHUB_REPOSITORY") != CODE_REPOSITORY
            or environ.get("GITHUB_REF") != "refs/heads/main"
            or environ.get("GITHUB_EVENT_NAME") not in ("workflow_dispatch", "schedule")
            or environ.get("ZOH_CLOUD_ENABLED") != "true"
            or not re.fullmatch(r"[1-9][0-9]*", environ.get("GITHUB_RUN_ID", ""))
            or not _HASH.fullmatch(expected)):
        raise CloudRunError("The reviewed main-branch cloud workflow is not enabled.")
    return expected


def within_session_window(now):
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise CloudRunError("A timezone-aware cloud clock is required.")
    local = now.astimezone(NY)
    return local.weekday() < 5 and (9, 0) <= (local.hour, local.minute) < (17, 30)


def environment_paper_credentials():
    # Only explicitly provisioned PAPER workflow secrets; no Windows fallback,
    # secret files, logging, dynamic endpoints, or alternative credential stores.
    value = {"key": normalize_secret_text(os.environ.get("ZOH_MAIN_PAPER_KEY", "")),
             "secret": normalize_secret_text(os.environ.get("ZOH_MAIN_PAPER_SECRET", ""))}
    if any(not isinstance(v, str) or not v or not v.isascii() or any(c.isspace() for c in v)
           for v in value.values()):
        raise CloudRunError("The authorized PAPER workflow secrets are unavailable.")
    return value


class _EvidenceBroker:
    """Record the same calendar response used by the unchanged signal function."""
    def __init__(self, broker, archive, run_id):
        self._broker, self._archive, self._run_id = broker, archive, run_id

    def __getattr__(self, name):
        return getattr(self._broker, name)

    def calendar(self, start, end):
        result = self._broker.calendar(start, end)
        self._archive.dataset(self._run_id, "trading_calendar", result,
                              metadata={"requested_start": start, "requested_end": end,
                                        "observed_at": datetime.now(timezone.utc).isoformat()})
        return result


class CloudCoordinator:
    """Single expected-HEAD owner, injectable synthetic stores and broker.

    Production entry uses restore(); the explicit constructor is also useful to
    offline tests. No operation here bootstraps a missing remote branch. Once a
    remote write/confirmation is uncertain the object is permanently poisoned;
    only a NEW run restoring/reconciling the current remote head may continue.
    """
    def __init__(self, core, bundle, directory, state, receipt, expected_account_sha256):
        self.core, self.bundle = core, bundle
        self.directory, self.state = Path(directory), state
        self.expected_account_sha256 = expected_account_sha256
        self.receipt, self.failed, self.evidence_ready = receipt, False, False
        self.archive_degraded = False
        self.archive, self.run_id = None, None
        if (not isinstance(receipt, CheckpointReceipt)
                or receipt.state_sha256 != hashlib.sha256(_canonical(state)).hexdigest()):
            raise CloudRunError("A matching restored core checkpoint is required.")
        self._validate_binding()
        archive_path = self.directory / "archive" / "research.sqlite3"
        if not archive_path.is_file():
            raise CloudRunError("The complete migrated archive is required.")
        with closing(sqlite3.connect(archive_path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            cutoff = db.execute("SELECT COALESCE(MAX(sequence),0) FROM events").fetchone()[0]
        if cutoff < self.proof["evidence_cutoff"]:
            raise CloudRunError("The migrated archive does not reach the cutover evidence cutoff.")

    @classmethod
    def restore(cls, core, bundle, directory, expected_account_sha256):
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        if any(directory.iterdir()):
            raise CloudRunError("Cloud restoration requires a new empty private runtime.")
        archive_path = directory / "archive" / "research.sqlite3"
        archive_path.parent.mkdir()
        recovered = bundle.restore_recovery(archive_path)
        restore_auxiliary(archive_path, directory)
        result = cls(core, bundle, directory, deepcopy(recovered.state), recovered.receipt,
                     expected_account_sha256)
        write_json(directory / "state.json", result.state)
        return result

    def _validate_binding(self):
        self.proof = read_json(self.directory / "cloud-cutover.json")
        validate_cutover(self.proof, self.state, self.expected_account_sha256)

    def _available(self, state):
        if self.failed:
            raise CloudDurabilityError("Remote persistence is uncertain; this execution cannot continue.")
        _check_state(state)
        if not hmac.compare_digest(account_digest(state), self.expected_account_sha256):
            self.failed = True
            raise CloudDurabilityError("The persisted account binding changed.")

    def _accept(self, receipt, state):
        previous = self.receipt
        if (not isinstance(receipt, CheckpointReceipt)
                or receipt.state_sha256 != hashlib.sha256(_canonical(state)).hexdigest()
                or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", receipt.commit)):
            raise CloudDurabilityError("Remote confirmation did not match the proposed checkpoint.")
        same = (receipt.commit, receipt.previous_commit, receipt.state_sha256) == (
            previous.commit, previous.previous_commit, previous.state_sha256)
        if not same and receipt.previous_commit != previous.commit:
            raise CloudDurabilityError("Remote confirmation did not continue the expected checkpoint.")
        self.receipt = receipt
        return receipt

    def checkpoint(self, state):
        self._available(state)
        try:
            # commit() includes exact remote readback. No automatic retry here.
            receipt = self.core.commit(state, expected_head=self.receipt.commit)
            return self._accept(receipt, state)
        except BaseException:
            self.failed = True
            raise CloudDurabilityError("Core persistence was not confirmed; execution must stop.") from None

    def evidence_barrier(self, state):
        self._available(state)
        if self.archive is None or self.run_id is None:
            raise AuxiliaryError("A current archive run is required for auxiliary persistence.")
        # Local-only preparation failure has made no remote/broker write. Core
        # persistence may still protect exits; entries stay blocked by the engine.
        try:
            capture_auxiliary(self.archive, self.run_id, self.directory)
        except Exception:
            self._archive_only_failure()
            raise AuxiliaryError("Local archive preparation failed; new entries remain blocked.") from None
        try:
            receipt = self.bundle.commit(state, self.archive.path, expected_head=self.receipt.commit)
            if not isinstance(receipt, BundleReceipt):
                raise CloudDurabilityError("Confirmed complete bundle evidence is required.")
            self._accept(receipt, state)
            self.evidence_ready = True
            return receipt
        except BundleCorrupt:
            # This class describes pre-submit export/old-evidence verification
            # failures. Post-push readback failure is BundleUncertain and MUST
            # take the poisoned path below. Confirm the unchanged prior core
            # before allowing any later protective operation.
            self._archive_only_failure()
            raise AuxiliaryError("Archive evidence failed verification; new entries remain blocked.") from None
        except BaseException:
            self.failed = True
            raise CloudDurabilityError("Bundle persistence was not confirmed; execution must stop.") from None

    def _archive_only_failure(self):
        try:
            loaded = self.core.confirm(self.receipt)
            if (not isinstance(loaded, LoadedCheckpoint)
                    or (loaded.receipt.commit, loaded.receipt.previous_commit, loaded.receipt.state_sha256) !=
                       (self.receipt.commit, self.receipt.previous_commit, self.receipt.state_sha256)
                    or hashlib.sha256(_canonical(loaded.state)).hexdigest() != self.receipt.state_sha256
                    or not hmac.compare_digest(account_digest(loaded.state), self.expected_account_sha256)):
                raise ValueError
        except BaseException:
            self.failed = True
            raise CloudDurabilityError("Core confirmation failed after an archive error; execution must stop.") from None
        self.archive_degraded, self.evidence_ready = True, False

    def _bundle_for(self, engine):
        if self.archive_degraded:
            return None
        try:
            result = self.evidence_barrier(deepcopy(self.state))
            engine._confirmed(result, self.state)
            return result
        except AuxiliaryError:
            engine.audit_available = False
            engine.alerts.append("Archive evidence unavailable; new entries blocked, confirmed core risk checks continue.")
            return None

    def _state_operation(self, engine, operation):
        before = _canonical(self.state)
        try:
            return operation()
        finally:
            # learn/apply write state directly; even a None-returning discard or
            # partial local failure must not lose an already-mutated state.
            if _canonical(self.state) != before:
                engine.save()

    def run_cycle(self, broker, config, *, operations=runner):
        """One natural cycle using existing strategies and CloudEngine barriers."""
        self._available(self.state)
        self._validate_binding()
        validate_config(config)
        snapshot, engine, finished = None, None, False
        self.archive_degraded = False
        with process_lock(self.directory):
            try:
                try:
                    self.archive = ResearchArchive(self.directory)
                    self.run_id = self.archive.start_run("cloud_cycle", config, self.state["strategies"])
                except Exception:
                    self.archive, self.run_id = None, None
                observed_broker = _EvidenceBroker(broker, self.archive, self.run_id)
                engine = CloudEngine(observed_broker, config, self.state, self.directory,
                                     checkpoint=self.checkpoint, evidence_barrier=self.evidence_barrier,
                                     audit=self.archive, run_id=self.run_id)
                if self.archive is None:
                    engine.audit_available = False
                    engine.alerts.append("Research archive unavailable; new entries blocked.")
                snapshot = engine.cycle({}, execute=True)  # risk/exits before data or research
                if self.archive is None:
                    self._archive_only_failure()
                    return {"mode": "PAPER", "status": "completed_with_alerts", "archive_complete": False,
                            "market_open": bool(snapshot["market_open"]), "paused": bool(self.state["paused"]),
                            "halted": bool(self.state["halted"])}
                now = timestamp(snapshot["at"]).astimezone(NY)
                try:
                    operations.bootstrap(self.archive, self.run_id, self.directory, observed_broker)
                    self._state_operation(engine, lambda: operations.capture_fills(
                        self.archive, self.run_id, observed_broker, self.directory, self.state, now))
                except CloudDurabilityError:
                    raise
                except Exception:
                    engine.audit_available = False
                    engine.alerts.append("Trade evidence collection failed; new entries blocked.")
                    engine.audit_event("cloud_collection_failed", {"new_entries": "blocked"})
                # Recovery's plain receipt never enables entries. Persist fresh
                # reconciliation and current evidence before any signal cycle.
                if not self.evidence_ready and engine.audit_available:
                    self._bundle_for(engine)
                research = read_json(self.directory / "research.json", {})
                if engine.audit_available:
                    before = deepcopy(self.state["strategies"])
                    applied = self._state_operation(engine, lambda: operations.apply_pending_research(
                        self.state, self.directory, now, snapshot["market_open"]))
                    if applied is not None:
                        research = applied
                        self.archive.event(self.run_id, "research_application", {
                            "results": applied, "strategies_before": before,
                            "strategies_after": self.state["strategies"]})
                due = engine.audit_available and operations.weekly_research_due(
                    now, self.state["last_research"], snapshot["market_open"])
                signals = None
                if snapshot["market_open"] or due:
                    try:
                        daily, minute = operations.refresh(observed_broker, config, self.directory, now, full=due)
                        metadata = {"feed": "iex", "adjustment": "split", "observed_at": datetime.now(timezone.utc).isoformat(),
                                    "requested_asof": now.isoformat(), "cache_meta": read_json(self.directory / "data_meta.json", {})}
                        self.archive.dataset(self.run_id, "daily", daily, metadata={**metadata, "timeframe": "1Day"})
                        self.archive.dataset(self.run_id, "intraday", minute, metadata={**metadata, "timeframe": "15Min"})
                        if due:
                            before = deepcopy(self.state["strategies"])
                            research = self._state_operation(engine, lambda: operations.learn(
                                self.state, daily, minute, self.directory, now, defer=True))
                            self.archive.event(self.run_id, "research_result", {"results": research,
                                "strategies_before": before, "strategies_after": self.state["strategies"]})
                        now = timestamp(observed_broker.clock()["timestamp"]).astimezone(NY)
                        signals = operations.make_signals(observed_broker, self.state, daily, minute, now)
                        self.archive.samples(self.run_id, operations.build_samples(daily, minute, config, self.state, signals, now))
                        self.archive.event(self.run_id, "selection", {"asof": now.isoformat(), "signals": signals,
                                                                    "strategies": self.state["strategies"]})
                    except CloudDurabilityError:
                        raise
                    except Exception:
                        signals = None
                        engine.alerts.append("Selection or research failed; new entries blocked for this cycle.")
                        engine.audit_event("selection_failed", {"asof": now.isoformat(), "new_entries": "blocked"})
                    if signals is not None:
                        if not self.evidence_ready:
                            engine.audit_available = False
                        snapshot = engine.cycle(signals, execute=True)
                snapshot["last_research"] = self.state["last_research"]
                snapshot.update(paused=self.state['paused'], halted=self.state['halted'], events=self.state['events'][-30:])
                if any(v.get('manual_review_required') for v in self.state['strategies'].get('options', {}).get('lifecycle_reviews', {}).values()):
                    engine.alerts.append('Option lifecycle activity requires manual inventory reconciliation; new entries remain paused.')
                snapshot["alerts"] = list(dict.fromkeys(snapshot.get("alerts", []) + engine.alerts))
                if signals is not None:
                    write_json(self.directory / "selection.json", {"at": now.isoformat(), "signals": signals})
                write_json(self.directory / "latest.json", snapshot)
                journal = read_json(self.directory / "equity-history.json", [])
                journal.append({key: snapshot[key] for key in ("at", "equity", "cash", "drawdown", "sleeves")})
                write_json(self.directory / "equity-history.json", journal[-10000:])
                self.archive.event(self.run_id, "equity_observation", journal[-1])
                self.archive.finish_run(self.run_id, "completed_with_alerts" if snapshot["alerts"] else "completed", snapshot=snapshot)
                finished = True
                self._bundle_for(engine)  # includes final run update and all auxiliary changes
                return {"mode": "PAPER", "status": "completed_with_alerts" if snapshot["alerts"] or self.archive_degraded else "completed",
                        "archive_complete": not self.archive_degraded,
                        "market_open": bool(snapshot["market_open"]), "paused": bool(self.state["paused"]),
                        "halted": bool(self.state["halted"])}
            except BaseException:
                # Never retry a broker write or a failed/uncertain persistence
                # callback. A new invocation must restore current remote state.
                if self.archive is not None and self.run_id is not None and not finished:
                    try:
                        self.archive.finish_run(self.run_id, "failed", snapshot=snapshot,
                                                error="Cloud cycle interrupted or failed; reconciliation required.")
                    except Exception:
                        pass
                if engine is not None and not self.failed and not engine._durability_failed:
                    try:
                        self._bundle_for(engine)
                    except BaseException:
                        pass
                raise CloudRunError("Cloud cycle stopped; restore and reconcile before further execution.") from None
            finally:
                if self.archive is not None:
                    self.archive.close()
                self.archive, self.run_id = None, None


def run_duration(coordinator, broker, config, duration_seconds, *, clock=time.monotonic,
                 sleeper=time.sleep, wall_clock=lambda: datetime.now(timezone.utc), deadline=None):
    """Stop admitting cycles at the soft deadline; never kill an active cycle.

    The CLI supplies a deadline established before cold restoration. A cycle
    already in progress may finish after it, so the workflow must retain margin
    below its separate hard timeout for persistence and cleanup.
    """
    if type(duration_seconds) is not int or not 60 <= duration_seconds <= 18000:
        raise CloudRunError("Cloud execution requires a reviewed bounded duration.")
    local_deadline = clock() + duration_seconds
    if deadline is not None:
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise CloudRunError("A finite cloud execution deadline is required.")
        local_deadline = min(local_deadline, deadline)
    deadline, cycles, verified_cycles = local_deadline, 0, 0
    had_alerts = False
    while clock() < deadline and within_session_window(wall_clock()):
        result = coordinator.run_cycle(broker, config)
        cycles += 1
        if result.get("archive_complete") is True and result.get("status") == "completed":
            verified_cycles += 1
        had_alerts = had_alerts or result.get("status") != "completed"
        remaining = deadline - clock()
        if remaining <= 0:
            break
        local = wall_clock().astimezone(NY)
        window_remaining = (local.replace(hour=17, minute=30, second=0, microsecond=0) - local).total_seconds()
        if window_remaining <= 0:
            break
        sleeper(min(60 if result["market_open"] else 300, remaining, window_remaining))
    return {"mode": "PAPER", "status": "completed_with_alerts" if had_alerts else "completed",
            "cycles": cycles, "verified_cycles": verified_cycles}


def main(argv=None, *, environ=None, core_factory=PrivateGithubCheckpointStore,
         bundle_factory=GitBundleStore, broker_factory=PaperBroker,
          wall_clock=lambda: datetime.now(timezone.utc), clock=time.monotonic, sleeper=time.sleep):
    """Safe CLI surface; all errors have a fixed public field/value whitelist."""
    try:
        started_at = clock()
        parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
        parser.add_argument("--duration-seconds", type=int, default=18000)
        args, extra = parser.parse_known_args(argv)
        if extra or not 60 <= args.duration_seconds <= 18000:
            raise CloudRunError("Unsupported cloud command.")
        deadline = started_at + args.duration_seconds
        environment = os.environ if environ is None else environ
        expected = validate_environment(environment)
        if not within_session_window(wall_clock()):
            print(json.dumps({"mode": "PAPER", "status": "outside_session", "cycles": 0}, sort_keys=True))
            return 0
        runner_temp = Path(environment.get("RUNNER_TEMP", ""))
        workspace = Path(environment.get("GITHUB_WORKSPACE", ""))
        if (not runner_temp.is_absolute() or not runner_temp.is_dir() or not workspace.is_absolute()
                or runner_temp.resolve().is_relative_to(workspace.resolve())):
            raise CloudRunError("A private temporary runtime outside the public workspace is required.")
        config = read_json(ROOT / "settings.json")
        validate_config(config)
        with tempfile.TemporaryDirectory(prefix="zoh-private-cloud-", dir=runner_temp) as temporary:
            directory = Path(temporary)
            directory.chmod(0o700)
            core = core_factory()
            try:
                with bundle_factory(core) as bundle:
                    coordinator = CloudCoordinator.restore(core, bundle, directory, expected)
                    # Cold recovery consumes the same CLI budget. Recheck the
                    # wall-clock window before even constructing a broker.
                    if not within_session_window(wall_clock()):
                        result = {"mode": "PAPER", "status": "outside_session", "cycles": 0,
                                  "verified_cycles": 0}
                    elif clock() >= deadline:
                        result = {"mode": "PAPER", "status": "budget_exhausted", "cycles": 0,
                                  "verified_cycles": 0}
                    else:
                        # Instantiate only after restore, cutover and account checks.
                        broker = broker_factory(credential_loader=environment_paper_credentials)
                        result = run_duration(coordinator, broker, config, args.duration_seconds,
                                              wall_clock=wall_clock, clock=clock, sleeper=sleeper,
                                              deadline=deadline)
            finally:
                core.close()
        print(json.dumps(result, sort_keys=True))
        return 0
    except BaseException:
        print(json.dumps({"mode": "PAPER", "status": "stopped", "new_entries": "blocked",
                          "action": "restore_and_reconcile"}, sort_keys=True))
        return 1


def _terminate(signum, frame):
    raise CloudRunInterrupted("The bounded cloud execution was interrupted.")


def cli():
    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGINT, _terminate)
    return main()
