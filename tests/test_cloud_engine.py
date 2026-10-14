"""Cloud adapter fault injection. Synthetic brokers/callbacks, no network."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import unittest

import test_engine
from test_engine_audit import FakeAudit
from zerotrader.broker import OrderAmbiguous
from zerotrader.cloud_bundle import BundleReceipt
from zerotrader.cloud_checkpoint import CheckpointReceipt, _canonical, _check_state, _transition
from zerotrader.cloud_engine import CloudDurabilityError, CloudEngine


@dataclass(frozen=True)
class SyntheticBundleReceipt(CheckpointReceipt):
    archive_manifest_sha256: str = "a" * 64
    archive_snapshot_sha256: str = "b" * 64
    archive_index_sha256: str = "c" * 64
    generation: int = 1


class SyntheticDurableStore:
    def __init__(self):
        self.state = None
        self.receipt = None
        self.commits = []
        self.before = None
        self.after = None
        self.archive_before = None
        self.archive_failed = False
        self.barriers = 0

    def checkpoint(self, state):
        if self.before:
            self.before(state)
        _check_state(state)
        if self.state is not None:
            _transition(self.state, state)
        previous = self.receipt.commit if self.receipt else None
        commit = format(len(self.commits) + 1, "040x")
        self.state = deepcopy(state)
        self.commits.append(deepcopy(state))
        self.receipt = CheckpointReceipt(commit, previous, hashlib.sha256(_canonical(state)).hexdigest())
        if self.after:
            self.after(state)
        return self.receipt

    def evidence(self, state):
        self.barriers += 1
        if self.archive_before:
            self.archive_before(state)
        if self.archive_failed:
            raise OSError("Synthetic evidence storage outage")
        receipt = self.checkpoint(state)
        return SyntheticBundleReceipt(receipt.commit, receipt.previous_commit, receipt.state_sha256,
                                      generation=self.barriers)


class CloudEngineTests(test_engine.EngineTests):
    """Run the existing engine risk cases against the opt-in adapter as well."""

    def setUp(self):
        super().setUp()
        self.state["account_id"] = self.broker.account_value["id"]
        self.remote = SyntheticDurableStore()
        self.audit = FakeAudit()

    def engine(self):
        return CloudEngine(self.broker, self.config, self.state, self.directory,
                           checkpoint=self.remote.checkpoint, evidence_barrier=self.remote.evidence,
                           audit=self.audit, run_id="synthetic-run")

    def restart_from_remote(self):
        self.state = deepcopy(self.remote.state)
        return self.engine()

    def reset_synthetic_state(self):
        self.state = test_engine.new_state()
        self.broker = test_engine.FakeBroker()
        self.state["account_id"] = self.broker.account_value["id"]
        self.remote = SyntheticDurableStore()
        self.audit = FakeAudit()

    def test_confirmed_intent_and_archived_context_precede_buy(self):
        original = self.broker.submit_order

        def inspect(payload):
            intent = self.remote.state["intents"][payload["client_order_id"]]
            self.assertEqual(intent["payload"], payload)
            self.assertEqual(intent["status"], "submitting")
            self.assertGreaterEqual(self.remote.barriers, 2)
            events = [row for _, kind, row in self.audit.events if kind == "order_intent"]
            self.assertEqual(events[-1]["context"]["quote"]["ap"], 100.01)
            return original(payload)

        self.broker.submit_order = inspect
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)

    def test_core_failure_before_intent_confirmation_prevents_post(self):
        def fail(state):
            if state["intents"]:
                raise OSError("Synthetic private failure text")

        self.remote.before = fail
        with self.assertRaises(CloudDurabilityError) as caught:
            self.engine().cycle(self.signals())
        self.assertNotIn("Synthetic private failure text", str(caught.exception))
        self.assertEqual(self.broker.submitted, [])

    def test_boolean_checkpoint_is_not_confirmation(self):
        engine = CloudEngine(self.broker, self.config, self.state, self.directory,
                             checkpoint=lambda state: True, evidence_barrier=self.remote.evidence)
        with self.assertRaises(CloudDurabilityError):
            engine.cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])

    def test_generation_zero_bundle_and_same_head_core_receipt_are_valid(self):
        digest = hashlib.sha256(_canonical(self.state)).hexdigest()
        receipt = BundleReceipt("1" * 40, None, digest, "a" * 64, "b" * 64, "c" * 64, 0)
        engine = CloudEngine(self.broker, self.config, self.state, self.directory,
                             checkpoint=lambda state: receipt.checkpoint, evidence_barrier=lambda state: receipt)
        self.assertTrue(engine._archive_barrier())
        engine.save()  # A confirmed no-op need not keep the BundleReceipt subclass.
        self.assertFalse(engine._durability_failed)
        self.assertEqual(engine._last_receipt, receipt.checkpoint)

    def test_archive_barrier_failure_blocks_buy_even_with_local_audit(self):
        self.remote.archive_failed = True
        snapshot = self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])
        self.assertTrue(snapshot["alerts"])
        self.assertEqual(next(iter(self.remote.state["intents"].values()))["status"], "not_submitted")

    def test_checkpoint_delay_expires_quote_before_buy(self):
        def delay(state):
            if any(intent["payload"]["side"] == "buy" for intent in state["intents"].values()):
                self.remote.after = None
                self.broker.now += timedelta(seconds=self.config["max_quote_age_seconds"] + 1)

        self.remote.after = delay
        self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(next(iter(self.remote.state["intents"].values()))["status"], "not_submitted")

    def test_final_evidence_delay_is_checked_again(self):
        def delay(state):
            if self.remote.barriers == 2:
                self.broker.now += timedelta(seconds=self.config["max_quote_age_seconds"] + 1)

        self.remote.archive_before = delay
        self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])

    def test_final_evidence_barrier_cannot_hide_cash_change_or_clock_reversal(self):
        for failure in ("cash", "backward_clock"):
            with self.subTest(failure=failure):
                self.reset_synthetic_state()

                def change(state):
                    if self.remote.barriers == 2:
                        if failure == "cash":
                            self.broker.account_value["cash"] = "1"
                        else:
                            self.broker.now -= timedelta(seconds=1)

                self.remote.archive_before = change
                self.engine().cycle(self.signals())
                self.assertEqual(self.broker.submitted, [])

    def test_cash_change_during_initial_persistence_blocks_oversized_buy(self):
        def change_cash(state):
            if state["intents"]:
                self.broker.account_value["cash"] = "1"
                self.remote.after = None

        self.remote.after = change_cash
        self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])

    def test_harmless_market_movement_during_final_barrier_allows_entry(self):
        self.owned()

        def move_market(state):
            if self.remote.barriers == 2:
                self.broker.now += timedelta(seconds=1)
                self.broker.account_value["equity"] = "100010"
                self.broker.position_rows[0]["market_value"] = "1010"
                self.broker.records["seed-stop"]["updated_at"] = self.broker.now.isoformat()

        self.remote.archive_before = move_market
        self.engine().cycle(self.signals(swing=("AAPL", "MSFT")))
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.broker.submitted[0]["symbol"], "MSFT")
        self.assertEqual(self.remote.state["equity_peak"], 100010)
        guards = [row for _, kind, row in self.audit.events if kind == "cloud_pre_submit_guard"]
        self.assertEqual(guards[0]["account"]["equity"], "100000")

    def test_nonfinite_or_negative_cash_cannot_bypass_cost_check(self):
        for bad in ("NaN", "Infinity", "-1"):
            with self.subTest(cash=bad):
                self.reset_synthetic_state()

                def corrupt(state):
                    if state["intents"]:
                        self.remote.after = None
                        self.broker.account_value["cash"] = bad

                self.remote.after = corrupt
                self.engine().cycle(self.signals())
                self.assertEqual(self.broker.submitted, [])

    def test_nonfinite_position_and_order_inputs_fail_closed(self):
        cases = [("position", key) for key in ("qty", "market_value", "cost_basis")]
        cases += [("order", key) for key in ("qty", "filled_qty", "limit_price")]
        for kind, key in cases:
            with self.subTest(kind=kind, key=key):
                self.reset_synthetic_state()

                def corrupt(state):
                    if not state["intents"]:
                        return
                    self.remote.after = None
                    if kind == "position":
                        row = {"symbol": "MSFT", "qty": "1", "market_value": "100", "cost_basis": "100"}
                        row[key] = "NaN"
                        self.broker.position_rows.append(row)
                    else:
                        row = {"id": "synthetic-late-order", "symbol": "MSFT", "side": "buy", "status": "new",
                               "type": "limit", "qty": "1", "filled_qty": "0", "limit_price": "100"}
                        row[key] = "NaN"
                        self.broker.records[row["id"]] = row

                self.remote.after = corrupt
                self.engine().cycle(self.signals())
                self.assertEqual(self.broker.submitted, [])

    def test_latched_halt_reason_and_peak_survive_restart(self):
        self.owned()
        self.state["halted"] = "Synthetic earlier latched halt"
        self.state["equity_peak"] = 110000
        self.broker.account_value["equity"] = "90000"
        self.engine().cycle(self.signals())
        self.assertEqual(self.remote.state["halted"], "Synthetic earlier latched halt")
        self.assertEqual(self.remote.state["equity_peak"], 110000)
        self.restart_from_remote().cycle(self.signals())
        self.assertEqual(self.state["halted"], "Synthetic earlier latched halt")
        self.assertEqual(len(self.broker.submitted), 1)

    def test_exit_plan_is_remote_before_cancel_and_archive_outage_does_not_block_exit(self):
        self.owned()
        self.broker.account_value["equity"] = "90000"
        original = self.broker.cancel_order

        def inspect(order_id):
            exits = [intent for intent in self.remote.state["intents"].values() if intent["payload"]["side"] == "sell"]
            self.assertEqual(len(exits), 1)
            self.assertEqual(exits[0]["status"], "exit_prepared")
            self.assertEqual(exits[0]["payload"]["qty"], "10")
            self.assertIn(order_id, exits[0]["cancel_order_ids"])
            self.audit.fail.add("*")
            self.remote.archive_failed = True
            return original(order_id)

        self.broker.cancel_order = inspect
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.broker.submitted[0]["side"], "sell")
        self.assertEqual(self.remote.barriers, 0)

    def test_core_outage_before_exit_plan_confirmation_keeps_existing_stop(self):
        self.owned()
        self.broker.account_value["equity"] = "90000"

        def fail(state):
            if any(intent["status"] == "exit_prepared" for intent in state["intents"].values()):
                raise OSError("Synthetic critical-store outage")

        self.remote.before = fail
        with self.assertRaises(CloudDurabilityError):
            self.engine().cycle(self.signals())
        self.assertEqual(self.broker.canceled, [])
        self.assertEqual(self.broker.submitted, [])

    def test_locally_terminal_exit_that_is_still_open_is_not_canceled_or_duplicated(self):
        self.owned()
        self.broker.account_value["equity"] = "90000"
        payload = {"symbol": "AAPL", "side": "sell", "qty": "10", "type": "market", "time_in_force": "day",
                   "position_intent": "sell_to_close", "client_order_id": "zoh-synthetic-old-exit"}
        self.state["intents"][payload["client_order_id"]] = {
            "payload": payload, "order_id": "old-exit", "status": "filled", "sleeve": "swing",
            "created": self.broker.now.isoformat(), "exit_for": "zoh-seed-entry"}
        self.broker.records["old-exit"] = dict(payload, id="old-exit", status="new", filled_qty="0")
        snapshot = self.engine().cycle(self.signals())
        self.assertEqual(self.broker.canceled, [])
        self.assertEqual(self.broker.submitted, [])
        self.assertTrue(any("existing exit" in alert for alert in snapshot["alerts"]))

    def test_zero_holding_with_invalid_pending_quantity_does_not_truncate_or_cancel(self):
        for bad in ("NaN", "Infinity", "0.5", "-1"):
            with self.subTest(pending_quantity=bad):
                self.reset_synthetic_state()
                self.owned(status="new", filled=0, protective=None)
                self.broker.position_rows = []
                self.broker.account_value["equity"] = "90000"
                self.broker.records["seed-entry"]["qty"] = bad
                self.engine().cycle(self.signals())
                self.assertEqual(self.broker.canceled, [])
                self.assertEqual(self.broker.submitted, [])

    def test_core_outage_after_cancel_does_not_claim_an_exit_was_sent(self):
        self.owned()
        self.broker.account_value["equity"] = "90000"

        def fail(state):
            if self.broker.canceled and any(intent["status"] == "submitting" and intent["payload"]["side"] == "sell"
                                           for intent in state["intents"].values()):
                raise OSError("Synthetic critical-store outage")

        self.remote.before = fail
        with self.assertRaises(CloudDurabilityError):
            self.engine().cycle(self.signals())
        self.assertEqual(self.broker.canceled, ["seed-stop"])
        self.assertEqual(self.broker.submitted, [])
        original_id = next(cid for cid, intent in self.remote.state["intents"].items() if intent["payload"]["side"] == "sell")
        self.remote.before = None
        self.restart_from_remote().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.broker.submitted[0]["client_order_id"], original_id)

    def test_crash_after_broker_acceptance_reconciles_instead_of_duplicating(self):
        # Unlike a local dictionary rollback, recover the exact last REMOTE state.
        def fail(state):
            if self.broker.submitted and any(intent.get("order_id") for intent in state["intents"].values()):
                raise OSError("Synthetic response-checkpoint outage")

        self.remote.before = fail
        with self.assertRaises(CloudDurabilityError):
            self.engine().cycle(self.signals())
        self.assertEqual(next(iter(self.remote.state["intents"].values()))["status"], "submitting")
        self.remote.before = None
        self.restart_from_remote().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(next(iter(self.state["intents"].values()))["status"], "new")

    def test_crash_after_dispatch_checkpoint_before_post_never_resends_on_404(self):
        self.owned()
        self.broker.account_value["equity"] = "90000"

        def crash(state):
            if any(intent["status"] == "submitting" and intent["payload"]["side"] == "sell"
                   for intent in state["intents"].values()):
                raise KeyboardInterrupt("Synthetic abrupt runner death")

        self.remote.after = crash
        with self.assertRaises(KeyboardInterrupt):
            self.engine().cycle(self.signals())
        self.remote.after = None
        snapshot = self.restart_from_remote().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])
        self.assertTrue(any("Unresolved submission" in alert for alert in snapshot["alerts"]))
        self.assertEqual(len([intent for intent in self.state["intents"].values() if intent["payload"]["side"] == "sell"]), 1)

    def test_quantity_change_after_cancel_requires_new_immutable_exit_intent(self):
        self.owned()
        self.broker.account_value["equity"] = "90000"
        original = self.broker.cancel_order

        def fill_before_cancel(order_id):
            self.broker.position_rows[0]["qty"] = "6"
            return original(order_id)

        self.broker.cancel_order = fill_before_cancel
        self.engine().cycle(self.signals())
        exits = [intent for intent in self.remote.state["intents"].values() if intent["payload"]["side"] == "sell"]
        self.assertEqual(len(exits), 2)
        self.assertEqual(exits[0]["payload"]["qty"], "10")
        self.assertEqual(exits[0]["status"], "not_submitted")
        self.assertEqual(exits[1]["payload"]["qty"], "6")
        self.assertEqual(self.broker.submitted[0]["qty"], "6")
        self.assertNotEqual(exits[0]["payload"]["client_order_id"], exits[1]["payload"]["client_order_id"])


if __name__ == "__main__":
    unittest.main()
