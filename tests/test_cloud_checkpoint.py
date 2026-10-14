"""Offline integration tests: synthetic state and temporary local bare Git only."""

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from zerotrader.cloud_checkpoint import (
    CHECKPOINT_PATH, CheckpointConflict, CheckpointCorrupt, CheckpointError,
    CheckpointInvalid, CheckpointMissing, CheckpointUncertain, GitCheckpointStore,
)


def synthetic_state():
    return {
        "version": 1, "account_id": "synthetic-account-never-a-real-account",
        "equity_peak": 100123.45, "halted": "synthetic latched drawdown halt",
        "paused": True, "owners": {"SYN": {"sleeve": "swing", "entry_cid": "zoh-synthetic-original-id",
                                             "entry_day": "2026-01-02"}},
        "intents": {"zoh-synthetic-original-id": {
            "payload": {"client_order_id": "zoh-synthetic-original-id", "symbol": "SYN",
                        "qty": "1", "side": "buy", "type": "limit", "limit_price": "10.00"},
            "sleeve": "swing", "status": "submitting", "created": "2026-01-02T15:00:00+00:00",
        }}, "fills": {}, "strategies": {}, "events": [],
        "last_research": None, "last_cycle": None,
    }


@unittest.skipUnless(shutil.which("git"), "Git CLI is required for local bare-repository tests")
class GitCheckpointTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="zoh-checkpoint-tests-")
        self.addCleanup(self.temp.cleanup)
        self.remote = Path(self.temp.name) / "synthetic-remote.git"
        self.remote.mkdir()
        self.store = GitCheckpointStore(self.remote)
        self.store._checked(self.remote, "init", "--bare", "--quiet")
        self.state = synthetic_state()

    def bootstrap(self):
        return self.store.bootstrap(self.state)

    def assert_remote_state(self, state):
        loaded = GitCheckpointStore(self.remote).load()
        self.assertEqual(loaded.state, state)
        return loaded

    def rewrite_remote_file(self, content, path=CHECKPOINT_PATH):
        """Synthetic external writer for corruption/tree-preservation cases only."""
        with self.store._repository() as directory:
            head = self.store._head(directory)
            self.store._fetch(directory, head)
            self.store._checked(directory, "read-tree", head)
            if path != CHECKPOINT_PATH:
                raw = self.store._checked(directory, "show", head + ":" + CHECKPOINT_PATH)
                envelope = json.loads(raw)
                envelope["previous_commit"] = head
                envelope_bytes = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
                checkpoint_blob = self.store._checked(directory, "hash-object", "-w", "--stdin", data=envelope_bytes).decode().strip()
                self.store._checked(directory, "update-index", "--add", "--cacheinfo", "100644", checkpoint_blob, CHECKPOINT_PATH)
            blob = self.store._checked(directory, "hash-object", "-w", "--stdin", data=content).decode().strip()
            self.store._checked(directory, "update-index", "--add", "--cacheinfo", "100644", blob, path)
            tree = self.store._checked(directory, "write-tree").decode().strip()
            commit = self.store._checked(directory, "commit-tree", tree, "-p", head,
                                          "-m", "Synthetic fixture update").decode().strip()
            result = self.store._push(directory, commit)
            self.assertEqual(result.returncode, 0)
            return commit

    def test_missing_load_is_an_error_and_does_not_create_state(self):
        with self.assertRaises(CheckpointMissing):
            self.store.load()
        with self.store._repository() as directory:
            self.assertIsNone(self.store._head(directory))

    def test_explicit_bootstrap_and_new_store_preserve_recovery_state(self):
        receipt = self.bootstrap()
        loaded = self.assert_remote_state(self.state)
        self.assertEqual(loaded.receipt, receipt)
        self.assertIsNone(receipt.previous_commit)
        canonical = json.dumps(self.state, sort_keys=True, ensure_ascii=False,
                               allow_nan=False, separators=(",", ":")).encode()
        self.assertEqual(receipt.state_sha256, hashlib.sha256(canonical).hexdigest())
        self.assertNotIn(self.state["account_id"], repr(loaded))
        with self.assertRaises(CheckpointConflict):
            self.store.bootstrap(self.state)

    def test_committed_uncertainty_restores_same_intent_without_broker_retry(self):
        initial = self.bootstrap()
        proposed = deepcopy(self.state)
        intent = proposed["intents"]["zoh-synthetic-original-id"]
        intent["status"] = "uncertain"
        receipt = self.store.commit(proposed, expected_head=initial.commit)
        recovered = self.assert_remote_state(proposed)
        self.assertEqual(recovered.receipt.previous_commit, initial.commit)
        self.assertEqual(recovered.state["equity_peak"], self.state["equity_peak"])
        self.assertEqual(recovered.state["halted"], self.state["halted"])
        self.assertEqual(recovered.state["intents"]["zoh-synthetic-original-id"]["payload"],
                         self.state["intents"]["zoh-synthetic-original-id"]["payload"])
        self.assertEqual(self.store.confirm(receipt).state, proposed)

    def test_stale_head_is_rejected_without_overwriting_newer_state(self):
        initial = self.bootstrap()
        next_state = deepcopy(self.state)
        next_state["equity_peak"] += 10
        receipt = self.store.commit(next_state, expected_head=initial.commit)
        with self.assertRaises(CheckpointConflict):
            self.store.commit(self.state, expected_head=initial.commit)
        self.assertEqual(self.assert_remote_state(next_state).receipt, receipt)

    def test_racing_writer_wins_and_plain_push_cannot_replace_it(self):
        initial = self.bootstrap()
        competitor = GitCheckpointStore(self.remote)
        winner = deepcopy(self.state)
        winner["equity_peak"] += 20
        loser = deepcopy(self.state)
        loser["equity_peak"] += 30
        original_push = self.store._push

        def racing_push(directory, commit):
            competitor.commit(winner, expected_head=initial.commit)
            return original_push(directory, commit)

        with patch.object(self.store, "_push", side_effect=racing_push) as push:
            with self.assertRaises(CheckpointConflict):
                self.store.commit(loser, expected_head=initial.commit)
        self.assertEqual(push.call_count, 1)
        self.assert_remote_state(winner)

    def test_lost_push_response_is_confirmed_by_exact_readback(self):
        initial = self.bootstrap()
        original_push = self.store._push

        def lost_response(directory, commit):
            result = original_push(directory, commit)
            self.assertEqual(result.returncode, 0)
            raise CheckpointError("Synthetic lost acknowledgement")

        with patch.object(self.store, "_push", side_effect=lost_response) as push:
            receipt = self.store.commit(self.state, expected_head=initial.commit)
        self.assertEqual(push.call_count, 1)
        self.assertEqual(self.assert_remote_state(self.state).receipt, receipt)

    def test_interruption_after_push_and_failed_readback_recovers_from_receipt(self):
        initial = self.bootstrap()
        proposed = deepcopy(self.state)
        proposed["intents"]["zoh-synthetic-original-id"]["status"] = "uncertain"
        with patch.object(self.store, "confirm", side_effect=CheckpointError("Synthetic read outage")):
            with self.assertRaises(CheckpointUncertain) as caught:
                self.store.commit(proposed, expected_head=initial.commit)
        receipt = caught.exception.receipt
        fresh_store = GitCheckpointStore(self.remote)
        self.assertEqual(fresh_store.confirm(receipt).state, proposed)
        self.assertEqual(fresh_store.load().state["halted"], self.state["halted"])

    def test_rejected_push_never_returns_a_confirmed_checkpoint_or_retries(self):
        initial = self.bootstrap()
        with patch.object(self.store, "_push", return_value=subprocess.CompletedProcess([], 1, b"", b"")) as push:
            with self.assertRaises(CheckpointUncertain):
                self.store.commit(self.state, expected_head=initial.commit)
        self.assertEqual(push.call_count, 1)
        self.assertEqual(self.store.load().receipt, initial)

    def test_receipt_with_wrong_hash_is_not_accepted(self):
        receipt = self.bootstrap()
        with self.assertRaises(CheckpointCorrupt):
            self.store.confirm(replace(receipt, state_sha256="0" * 64))

    def test_corrupt_payload_cannot_be_loaded_even_with_valid_json(self):
        receipt = self.bootstrap()
        with self.store._repository() as directory:
            self.store._fetch(directory, receipt.commit)
            raw = self.store._checked(directory, "show", receipt.commit + ":" + CHECKPOINT_PATH)
        envelope = json.loads(raw)
        envelope["state"]["equity_peak"] = 1
        envelope["previous_commit"] = receipt.commit
        self.rewrite_remote_file(json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode())
        with self.assertRaises(CheckpointCorrupt):
            self.store.load()

    def test_valid_state_hash_does_not_hide_an_incorrect_parent_claim(self):
        receipt = self.bootstrap()
        with self.store._repository() as directory:
            self.store._fetch(directory, receipt.commit)
            raw = self.store._checked(directory, "show", receipt.commit + ":" + CHECKPOINT_PATH)
        # The copied root envelope claims no parent, but the new commit has one.
        self.rewrite_remote_file(raw)
        with self.assertRaises(CheckpointCorrupt):
            self.store.load()

    def test_noncheckpoint_files_are_preserved_and_parent_is_expected_head(self):
        self.bootstrap()
        head = self.rewrite_remote_file(b"synthetic unrelated evidence\n", "evidence/keep.txt")
        receipt = self.store.commit(self.state, expected_head=head)
        with self.store._repository() as directory:
            self.store._fetch(directory, receipt.commit)
            self.assertEqual(self.store._checked(directory, "show", receipt.commit + ":evidence/keep.txt"),
                             b"synthetic unrelated evidence\n")
            lineage = self.store._checked(directory, "rev-list", "--parents", "-n", "1", receipt.commit).decode().split()
            self.assertEqual(lineage, [receipt.commit, head])

    def test_account_halt_peak_pause_and_original_intent_cannot_be_reset(self):
        initial = self.bootstrap()
        changes = [
            lambda state: state.update(account_id="another-synthetic-account"),
            lambda state: state.update(equity_peak=100000),
            lambda state: state.update(halted=None),
            lambda state: state.update(paused=False),
            lambda state: state["intents"].clear(),
            lambda state: state["intents"]["zoh-synthetic-original-id"]["payload"].update(qty="2"),
        ]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                proposed = deepcopy(self.state)
                change(proposed)
                with self.assertRaises(CheckpointInvalid):
                    self.store.commit(proposed, expected_head=initial.commit)
        self.assertEqual(self.store.load().receipt, initial)

    def test_credential_fields_and_unbound_account_are_rejected_before_git(self):
        for key in ("key", "secret", "api_key", "api-secret", "token", "password", "APCA-API-KEY-ID"):
            proposed = deepcopy(self.state)
            proposed["intents"]["zoh-synthetic-original-id"][key] = "synthetic-secret-marker"
            with self.subTest(key=key), patch.object(self.store, "_git") as git:
                with self.assertRaises(CheckpointInvalid) as caught:
                    self.store.bootstrap(proposed)
                git.assert_not_called()
                self.assertNotIn("synthetic-secret-marker", str(caught.exception))
        proposed = deepcopy(self.state)
        proposed["account_id"] = None
        with self.assertRaises(CheckpointInvalid):
            self.store.bootstrap(proposed)

    def test_existing_order_lineage_and_known_children_are_never_erased(self):
        original = self.state["intents"]["zoh-synthetic-original-id"]
        original.update(order_id="synthetic-parent-order", status="filled")
        exit_intent = deepcopy(original)
        exit_intent["payload"].update(client_order_id="zoh-synthetic-exit-id", side="sell")
        exit_intent.update(order_id="synthetic-exit-order", status="new", exit_for="zoh-synthetic-original-id")
        self.state["intents"]["zoh-synthetic-exit-id"] = exit_intent
        self.state["known_order_ids"] = ["synthetic-parent-order", "synthetic-protective-child", "synthetic-exit-order"]
        receipt = self.bootstrap()
        for name in ("order_id", "sleeve", "exit_for", "created"):
            for operation in ("delete", "replace"):
                proposed = deepcopy(self.state)
                intent = proposed["intents"]["zoh-synthetic-exit-id"]
                if operation == "delete":
                    del intent[name]
                else:
                    intent[name] = "synthetic-replacement"
                with self.subTest(field=name, operation=operation), self.assertRaises(CheckpointInvalid):
                    self.store.commit(proposed, expected_head=receipt.commit)
        for removed in (None, [], ["synthetic-parent-order"]):
            proposed = deepcopy(self.state)
            if removed is None:
                del proposed["known_order_ids"]
            else:
                proposed["known_order_ids"] = removed
            with self.subTest(known_order_ids=removed), self.assertRaises(CheckpointInvalid):
                self.store.commit(proposed, expected_head=receipt.commit)
        self.assertEqual(self.store.load().receipt, receipt)
        proposed = deepcopy(self.state)
        proposed["known_order_ids"].append("synthetic-new-child")
        self.store.commit(proposed, expected_head=receipt.commit)
        self.assert_remote_state(proposed)

    def test_partial_or_wrongly_typed_state_is_never_a_recoverable_checkpoint(self):
        for name in self.state:
            proposed = deepcopy(self.state)
            del proposed[name]
            with self.subTest(missing=name), patch.object(self.store, "_git") as git:
                with self.assertRaises(CheckpointInvalid):
                    self.store.bootstrap(proposed)
                git.assert_not_called()
        for name, value in (("version", True), ("owners", []), ("fills", []),
                            ("strategies", []), ("events", {}), ("last_cycle", 1),
                            ("last_research", []), ("known_order_ids", {}),
                            ("pending_research", [])):
            proposed = deepcopy(self.state)
            proposed[name] = value
            with self.subTest(wrong_type=name), self.assertRaises(CheckpointInvalid):
                self.store.bootstrap(proposed)

    def test_credential_bearing_remote_and_unsafe_branch_are_rejected(self):
        for remote in ("https://token@example.test/private.git", "https://user:secret@example.test/r.git",
                       "https://example.test/r.git?token=secret", "https://example.test/r.git#secret",
                       "http://example.test/r.git", "relative.git"):
            with self.subTest(remote=remote), self.assertRaises(CheckpointInvalid):
                GitCheckpointStore(remote)
        for branch in ("-option", "refs/../bad", "safe.lock/child", "bad\nname"):
            with self.subTest(branch=branch), self.assertRaises(CheckpointInvalid):
                GitCheckpointStore(self.remote, branch)

    def test_state_stays_off_argv_and_subprocess_diagnostics_are_sanitized(self):
        actual_run = subprocess.run
        calls = []

        def observe(args, **kwargs):
            calls.append(args)
            for marker in (self.state["account_id"], "zoh-synthetic-original-id", self.state["halted"]):
                self.assertNotIn(marker, " ".join(str(arg) for arg in args))
            return actual_run(args, **kwargs)

        with patch("zerotrader.cloud_checkpoint.subprocess.run", side_effect=observe):
            self.bootstrap()
        pushes = [args for args in calls if "push" in args]
        self.assertEqual(len(pushes), 1)
        for args in pushes:
            self.assertNotIn("--force", args)
            self.assertNotIn("--force-with-lease", args)
            self.assertFalse(any(str(arg).startswith("+") for arg in args))
        marker = "synthetic-private-error-content"
        with patch("zerotrader.cloud_checkpoint.subprocess.run",
                   return_value=subprocess.CompletedProcess([], 1, marker.encode(), marker.encode())):
            with self.assertRaises(CheckpointError) as caught:
                self.store.load()
        self.assertNotIn(marker, str(caught.exception))
        self.assertNotIn(marker, repr(caught.exception))


if __name__ == "__main__":
    unittest.main()
