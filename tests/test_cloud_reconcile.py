"""Synthetic GET transport / state comparison; no broker or GitHub connection."""
from contextlib import closing, redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
import urllib.parse
import urllib.request
from unittest.mock import patch

from zerotrader import cloud_reconcile as reconcile, runner
from zerotrader.broker import BrokerError
from zerotrader.cloud_bundle import BundleReceipt
from zerotrader.cloud_checkpoint import CheckpointReceipt, LoadedCheckpoint, _canonical
from zerotrader.cloud_runner import STATE_REPOSITORY, account_digest
from zerotrader.engine import new_state

NOW = datetime(2026, 10, 14, 21, 30, tzinfo=timezone.utc)
HEAD = "a" * 40
CID = "zoh-synthetic-entry"


class FakeBroker:
    def __init__(self):
        self.account_value = {"id": "synthetic-account", "status": "ACTIVE", "equity": "100000", "cash": "99900"}
        self.position_values = [{"symbol": "SYN", "qty": "10", "market_value": "100", "cost_basis": "100", "asset_class": "us_equity"}]
        self.stop = {"id": "stop-1", "client_order_id": "synthetic-child", "symbol": "SYN", "side": "sell",
                     "type": "stop", "status": "accepted", "qty": "10", "filled_qty": "0", "stop_price": "9.50"}
        self.parent = {"id": "parent-1", "client_order_id": CID, "symbol": "SYN", "side": "buy", "type": "limit",
                       "status": "filled", "qty": "10", "filled_qty": "10", "limit_price": "10.10", "legs": [self.stop]}
        self.open_values = [self.stop]
        self.activities = [{"id": "synthetic-activity", "order_id": "parent-1", "activity_type": "FILL", "symbol": "SYN",
                            "side": "buy", "qty": "10", "price": "10", "transaction_time": "2026-10-14T14:00:00Z"}]
        self.lookup = self.parent
        self.requests = []
        self.position_reads = 0
        self.change_inventory = False

    def account(self):
        return deepcopy(self.account_value)

    def clock(self):
        return {"timestamp": NOW.isoformat(), "is_open": False}

    def positions(self):
        self.position_reads += 1
        value = deepcopy(self.position_values)
        if self.change_inventory and self.position_reads > 1:
            value[0]["qty"] = "9"
        return value

    def orders(self):
        return deepcopy(self.open_values)

    def get_order(self, order_id):
        return deepcopy(self.lookup)

    def order_by_client_id(self, cid):
        return deepcopy(self.lookup)

    def fill_activities(self, after=None, until=None):
        self.bounds = (after, until)
        return deepcopy(self.activities)

    def transport(self, request, timeout):
        self.requests.append(request)
        parsed = urllib.parse.urlsplit(request.full_url)
        path = parsed.path
        if path == "/v2/account": value = self.account()
        elif path == "/v2/clock": value = self.clock()
        elif path == "/v2/positions": value = self.positions()
        elif path == "/v2/orders": value = self.orders()
        elif path == "/v2/orders/parent-1": value = self.get_order("parent-1")
        elif path == "/v2/orders:by_client_order_id": value = self.order_by_client_id(CID)
        elif path == "/v2/account/activities/FILL":
            # PaperBroker requests the terminal empty page too.
            query = urllib.parse.parse_qs(parsed.query)
            value = [] if "page_token" in query else self.activities
        else: raise AssertionError("Unexpected synthetic endpoint")
        return (404, b"") if value is None else (200, json.dumps(value).encode())


class ReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.broker = FakeBroker()
        self.state = new_state()
        self.state["account_id"] = self.broker.account_value["id"]
        runner.seed_strategies(self.state)
        self.state["owners"] = {"SYN": {"sleeve": "swing", "entry_cid": CID, "entry_day": "2026-10-14"}}
        self.state["intents"] = {CID: {"payload": {"client_order_id": CID, "symbol": "SYN", "side": "buy", "qty": "10", "limit_price": "10.10"},
            "order_id": "parent-1", "sleeve": "swing", "status": "filled", "created": "2026-10-14T14:00:00Z"}}
        self.state["fills"] = {"parent-1": {"symbol": "SYN", "side": "buy", "qty": 10, "price": 10, "sleeve": "swing"}}
        self.state["known_order_ids"] = ["parent-1", "stop-1"]
        self.expected = account_digest(self.state)
        self.config = json.loads((Path(__file__).resolve().parents[1] / "settings.json").read_text())
        self.cursor = {"last_successful_poll": "2026-10-14T20:00:00Z"}
        self.result = reconcile._result()

    def compare(self):
        original = _canonical(self.state)
        try:
            reconcile.compare_observations(self.state, self.broker, self.config, self.cursor, self.expected, self.result, now=NOW)
        finally:
            self.assertEqual(_canonical(self.state), original)

    def failure(self, category):
        with self.assertRaises(reconcile.ReconciliationFailure) as error:
            self.compare()
        self.assertEqual(error.exception.category, category)

    def test_bound_owned_quantity_stops_and_fills_pass_without_state_changes(self):
        self.compare()
        self.assertEqual(self.result["positions"], 1)
        self.assertEqual(self.result["queried_intents"], 1)
        self.assertTrue(self.result["fill_backfill_available"])
        self.assertEqual(self.broker.bounds[0], "2026-10-13T20:00:00+00:00")
        self.assertTrue(self.result["broker_inventory_stable"])
        self.assertFalse(self.result["continuous_protection_proven"])

    def test_uncertain_original_cid_404_is_failure_never_resubmitted(self):
        intent = self.state["intents"][CID]
        intent.pop("order_id")
        intent["status"] = "uncertain"
        self.broker.lookup = None
        self.failure("unresolved_intents")
        self.assertEqual(self.result["unresolved_intents"], 1)
        self.assertEqual(intent["status"], "uncertain")

    def test_uncertain_original_cid_found_is_observed_without_changing_intent(self):
        self.state["intents"][CID].pop("order_id")
        self.state["intents"][CID]["status"] = "uncertain"
        self.compare()
        self.assertEqual(self.state["intents"][CID]["status"], "uncertain")

    def test_unknown_position_is_not_adopted(self):
        self.broker.position_values.append({"symbol": "UNK", "qty": "1", "market_value": "1", "cost_basis": "1"})
        self.failure("unknown_activity")
        self.assertNotIn("UNK", self.state["owners"])

    def test_known_order_id_without_verified_owner_lineage_is_not_enough(self):
        stranger = dict(self.broker.stop, id="other-known-id")
        self.state["known_order_ids"].append("other-known-id")
        self.broker.open_values.append(stranger)
        self.failure("unknown_activity")

    def test_changed_parent_limit_identity_is_refused(self):
        self.broker.parent["limit_price"] = "10.20"
        self.failure("order_identity_mismatch")

    def test_owner_sleeve_must_match_persisted_entry(self):
        self.state["owners"]["SYN"]["sleeve"] = "intraday"
        self.failure("order_identity_mismatch")

    def test_stop_price_must_match_persisted_entry_terms_when_recorded(self):
        self.state["intents"][CID]["payload"]["stop_loss"] = {"stop_price": "9.00"}
        self.failure("order_identity_mismatch")

    def test_position_quantity_and_confirmed_stop_coverage_are_checked(self):
        self.broker.position_values[0]["qty"] = "11"
        self.failure("position_quantity_mismatch")
        self.broker.position_values[0]["qty"] = "10"
        self.broker.stop["qty"] = "9"
        self.result = reconcile._result()
        self.failure("protective_coverage_missing")

    def test_pending_stop_does_not_claim_confirmed_coverage(self):
        self.broker.stop["status"] = "pending_new"
        self.failure("protective_coverage_missing")

    def test_fill_identity_and_duplicate_activity_are_validated(self):
        self.broker.activities[0]["symbol"] = "OTHER"
        self.failure("unknown_activity")
        self.broker.activities[0]["symbol"] = "SYN"
        self.broker.activities.append(deepcopy(self.broker.activities[0]))
        self.result = reconcile._result()
        self.failure("broker_response_invalid")

    def test_nan_and_short_positions_fail_closed(self):
        for qty in ("NaN", "-1", "Infinity"):
            self.broker.position_values[0]["qty"] = qty
            self.failure("broker_response_invalid")

    def test_latched_halt_and_paused_are_preserved_and_block_enable(self):
        self.state["halted"] = "synthetic risk latch"
        self.state["paused"] = True
        self.failure("risk_or_account_blocks_enable")
        self.assertTrue(self.result["risk_latched"])
        self.assertTrue(self.result["paused"])

    def test_drawdown_peak_is_not_reset(self):
        self.state["equity_peak"] = 120000
        self.failure("risk_or_account_blocks_enable")
        self.assertEqual(self.state["equity_peak"], 120000)

    def test_inventory_change_during_reads_is_not_retried(self):
        self.broker.change_inventory = True
        self.failure("broker_changed_during_read")
        self.assertEqual(self.broker.position_reads, 2)

    def test_pending_buy_reservations_cannot_exceed_sleeve_capital_or_cash(self):
        for price, cash in (("6000", "100000"), ("900", "1000")):
            with self.subTest(price=price, cash=cash):
                self.broker.parent.update(status="new", filled_qty="0", limit_price=price, legs=[])
                self.state["intents"][CID]["payload"]["limit_price"] = price
                self.state["intents"][CID]["status"] = "new"
                self.broker.open_values = [self.broker.parent]
                self.broker.position_values = []
                self.broker.activities = []
                self.broker.account_value["cash"] = cash
                self.result = reconcile._result()
                self.failure("risk_or_account_blocks_enable")

    def test_pending_buy_without_limit_cannot_be_valued_or_pass(self):
        self.broker.parent.update(type="market", status="new", filled_qty="0", legs=[])
        self.broker.open_values = [self.broker.parent]
        self.broker.position_values = []
        self.broker.activities = []
        self.failure("broker_response_invalid")

    def test_transport_rejects_mutations_and_other_hosts_before_network(self):
        broker = reconcile.ReadOnlyPaperBroker(credential_loader=lambda: {"key": "synthetic-key", "secret": "synthetic-secret"}, transport=self.broker.transport)
        for method in ("POST", "PATCH", "DELETE"):
            with self.assertRaises(BrokerError): broker._request(method, "/v2/orders")
        with self.assertRaises(BrokerError): broker.cancel_order("parent-1")
        for url in ("https://api.alpaca.markets/v2/account", "https://paper-api.alpaca.markets/v2/positions/OTHER",
                    "https://paper-api.alpaca.markets/v2/account?unknown=1", "https://paper-api.alpaca.markets:443/v2/account"):
            with self.assertRaises(BrokerError): reconcile._allowed_request(urllib.request.Request(url))
        self.assertEqual(self.broker.requests, [])

    def test_readonly_private_store_write_hook_always_refuses(self):
        with self.assertRaises(reconcile.ReconciliationFailure):
            reconcile.ReadOnlyPrivateStore._write(object(), self.state, None)


class ReconciliationEntrypointTests(unittest.TestCase):
    def setUp(self):
        fixture = ReconciliationTests()
        fixture.setUp()
        self.state, self.broker, self.expected = fixture.state, fixture.broker, fixture.expected
        self.temp = tempfile.TemporaryDirectory(prefix="zoh-synthetic-readonly-reconcile-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": reconcile.CODE_REPOSITORY,
            "GITHUB_REF": "refs/heads/main", "GITHUB_EVENT_NAME": "workflow_dispatch", "ZOH_CLOUD_ENABLED": "false",
            "ZOH_MAIN_PAPER_KEY": "synthetic-key", "ZOH_MAIN_PAPER_SECRET": "synthetic-secret",
            "ZOH_STATE_GITHUB_TOKEN": "synthetic-state-token-long", "ZOH_EXPECTED_ACCOUNT_SHA256": self.expected,
            "ZOH_EXPECTED_CORE_COMMIT": HEAD, "RUNNER_TEMP": str(self.root), "GITHUB_WORKSPACE": str(self.root / "public")}
        self.receipt = CheckpointReceipt(HEAD, None, hashlib.sha256(_canonical(self.state)).hexdigest())
        self.core = SimpleNamespace(load=lambda: LoadedCheckpoint(self.receipt, deepcopy(self.state)),
            confirm=lambda receipt: LoadedCheckpoint(self.receipt, deepcopy(self.state)), close=lambda: None)
        receipt, state = self.receipt, self.state
        class Bundle:
            def __init__(self, core): pass
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def restore(self, target):
                with closing(sqlite3.connect(target)) as db:
                    db.execute("CREATE TABLE events(sequence INTEGER)")
                    db.execute("INSERT INTO events VALUES(1)")
                    db.commit()
                bundled = BundleReceipt(receipt.commit, receipt.previous_commit, receipt.state_sha256, "b"*64, "c"*64, "d"*64, 0)
                return SimpleNamespace(state=deepcopy(state), receipt=bundled)
        self.bundle = Bundle
        def restore_auxiliary(archive, directory):
            proof = {"version": 1, "authority": "github-actions", "code_repository": reconcile.CODE_REPOSITORY,
                "state_repository": STATE_REPOSITORY, "branch": "main", "account_sha256": self.expected,
                "local_worker_stopped": True, "local_logon_task_disabled": True, "stopped_at": "2026-10-14T20:00:00Z",
                "source_commit": "e"*40, "evidence_cutoff": 1}
            (directory / "cloud-cutover.json").write_text(json.dumps(proof))
            (directory / "fill-archive-cursor.json").write_text(json.dumps({"last_successful_poll": "2026-10-14T20:00:00Z"}))
        self.aux = patch.object(reconcile, "restore_auxiliary", side_effect=restore_auxiliary)
        self.aux.start()
        self.addCleanup(self.aux.stop)

    def run_check(self):
        return reconcile.run_reconciliation(environ=self.env, core_factory=lambda **kwargs: self.core,
                bundle_factory=self.bundle, paper_transport=self.broker.transport, now=NOW)

    def test_complete_readonly_path_safe_summary_and_exact_head_confirmation(self):
        original = _canonical(self.state)
        result = self.run_check()
        self.assertEqual(result["status"], "passed_readonly_reconciliation", result)
        self.assertTrue(result["state_unchanged"])
        self.assertTrue(result["remote_head_unchanged"])
        self.assertTrue(result["reconciliation_passed"])
        self.assertEqual(_canonical(self.state), original)
        self.assertEqual(set(result), set(reconcile._result()))
        for private in ("synthetic-account", CID, "parent-1", "stop-1", "synthetic-activity", self.expected, str(self.root), "synthetic-key"):
            self.assertNotIn(private, json.dumps(result))
        self.assertFalse(result["trading_enabled"])
        for request in self.broker.requests:
            self.assertEqual(request.get_method(), "GET")
            self.assertIsNone(request.data)

    def test_wrong_expected_head_prevents_all_broker_reads(self):
        self.env["ZOH_EXPECTED_CORE_COMMIT"] = "f" * 40
        result = self.run_check()
        self.assertEqual(result["error_category"], "checkpoint_mismatch")
        self.assertEqual(self.broker.requests, [])

    def test_remote_head_advance_after_reads_cannot_pass(self):
        other = CheckpointReceipt("f"*40, HEAD, self.receipt.state_sha256)
        self.core.confirm = lambda receipt: LoadedCheckpoint(other, deepcopy(self.state))
        result = self.run_check()
        self.assertEqual(result["error_category"], "remote_state_changed")
        self.assertFalse(result["reconciliation_passed"])

    def test_enabled_or_untrusted_context_stops_before_store_or_broker(self):
        for name, value in (("ZOH_CLOUD_ENABLED", "true"), ("GITHUB_EVENT_NAME", "schedule"),
                            ("GITHUB_REPOSITORY", "other/repository")):
            env = dict(self.env, **{name: value})
            with patch.object(reconcile, "ReadOnlyPrivateStore", side_effect=AssertionError("must not construct")):
                result = reconcile.run_reconciliation(environ=env, paper_transport=self.broker.transport)
            self.assertEqual(result["error_category"], "execution_context_invalid")
        self.assertEqual(self.broker.requests, [])

    def test_restore_failure_does_not_echo_error_or_read_broker(self):
        def broken(core): raise RuntimeError("SYNTHETIC PRIVATE RAW STATE")
        result = reconcile.run_reconciliation(environ=self.env, core_factory=lambda **kwargs: self.core,
                    bundle_factory=broken, paper_transport=self.broker.transport, now=NOW)
        self.assertEqual(result["status"], "failed")
        self.assertNotIn("SYNTHETIC", json.dumps(result))
        self.assertEqual(self.broker.requests, [])

    def test_post_validation_cleanup_failure_cannot_leave_success_status(self):
        def failed_close(): raise RuntimeError("SYNTHETIC PRIVATE ERROR")
        self.core.close = failed_close
        result = self.run_check()
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["reconciliation_passed"])
        self.assertNotIn("SYNTHETIC", json.dumps(result))

    def test_cli_rejects_arbitrary_inputs_without_echo(self):
        out = io.StringIO()
        with redirect_stdout(out): code = reconcile.main(["PRIVATE VALUE"])
        self.assertEqual(code, 1)
        self.assertNotIn("PRIVATE VALUE", out.getvalue())


if __name__ == "__main__":
    unittest.main()
