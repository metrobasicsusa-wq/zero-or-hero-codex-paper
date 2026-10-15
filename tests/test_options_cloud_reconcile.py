"""Synthetic read-only option inventory checks; never contact a broker."""
from copy import deepcopy
from datetime import datetime, timezone
import unittest

from zerotrader import cloud_reconcile as reconcile
from zerotrader.cloud_checkpoint import _canonical
from zerotrader.cloud_runner import account_digest
from zerotrader.engine import new_state


NOW = datetime(2026, 10, 15, 17, 0, tzinfo=timezone.utc)
SYMBOL = "SPY261030C00650000"
CID = "zoh-synthetic-option-entry"


class OptionBroker:
    def __init__(self):
        self.account_value = {"id": "synthetic-account", "status": "ACTIVE", "equity": "100000", "cash": "99800"}
        self.position_values = [{"symbol": SYMBOL, "qty": "1", "market_value": "210", "cost_basis": "200", "asset_class": "us_option"}]
        self.parent = {"id": "synthetic-parent", "client_order_id": CID, "symbol": SYMBOL, "asset_class": "us_option",
            "side": "buy", "type": "limit", "status": "filled", "qty": "1", "filled_qty": "1",
            "limit_price": "2.00", "position_intent": "buy_to_open"}
        self.open_values = []
        self.activities = [{"id": "synthetic-activity", "order_id": self.parent["id"], "activity_type": "FILL", "symbol": SYMBOL,
            "side": "buy", "qty": "1", "price": "2.00", "transaction_time": "2026-10-15T16:00:00Z"}]
        self.position_reads = 0
        self.mutate_second_read = False
        self.observed_at = NOW
        self.is_open = True
        self.close_override = None

    def account(self): return deepcopy(self.account_value)
    def clock(self):
        return {"timestamp": self.observed_at.isoformat(), "is_open": self.is_open,
                "next_close": self.close_override or self.observed_at.replace(hour=20, minute=0, second=0).isoformat()}
    def orders(self): return deepcopy(self.open_values)
    def get_order(self, order_id): return deepcopy(self.parent)
    def order_by_client_id(self, cid): return deepcopy(self.parent)
    def fill_activities(self, **kwargs): return deepcopy(self.activities)

    def positions(self):
        self.position_reads += 1
        value = deepcopy(self.position_values)
        if self.mutate_second_read and self.position_reads > 1:
            value[0]["asset_class"] = "us_equity"
        return value


class OptionsReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.broker = OptionBroker()
        self.config = {"capital": 100000, "max_drawdown": .10, "cash_reserve_fraction": .02,
            "swing": {"allocation": 50000}, "intraday": {"allocation": 49000},
            "options": {"enabled": True, "allocation": 1000, "fee_allowance_per_contract": 10}}
        contract = {"symbol": SYMBOL, "underlying_symbol": "SPY", "expiration_date": "2026-10-30",
            "strike_price": "650", "size": "100", "type": "call", "style": "american", "status": "active", "tradable": True}
        self.state = new_state()
        self.state["account_id"] = self.broker.account_value["id"]
        self.state["owners"] = {SYMBOL: {"sleeve": "options", "entry_cid": CID, "entry_day": "2026-10-15",
            "asset_class": "us_option", "multiplier": 100, "contract": deepcopy(contract)}}
        self.state["intents"] = {CID: {"payload": {"client_order_id": CID, "symbol": SYMBOL, "side": "buy", "qty": "1", "limit_price": "2.00"},
            "order_id": self.broker.parent["id"], "sleeve": "options", "status": "filled", "created": "2026-10-15T16:00:00Z",
            "asset_class": "us_option", "multiplier": 100, "contract": deepcopy(contract)}}
        self.state["fills"] = {self.broker.parent["id"]: {"symbol": SYMBOL, "side": "buy", "qty": 1, "price": 2, "sleeve": "options",
            "asset_class": "us_option", "multiplier": 100}}
        self.state["known_order_ids"] = [self.broker.parent["id"]]
        self.result = reconcile._result()

    def compare(self):
        before = _canonical(self.state)
        try:
            reconcile.compare_observations(self.state, self.broker, self.config, {}, account_digest(self.state), self.result, now=self.broker.observed_at)
        finally:
            self.assertEqual(_canonical(self.state), before)

    def failure(self, category=None):
        with self.assertRaises(reconcile.ReconciliationFailure) as context:
            self.compare()
        if category:
            self.assertEqual(context.exception.category, category)

    def pending(self, price="2.00"):
        self.broker.parent.update(status="new", filled_qty="0", limit_price=price)
        self.broker.open_values = [self.broker.parent]
        self.broker.position_values = []
        self.broker.activities = []
        self.state["fills"] = {}
        self.state["intents"][CID]["status"] = "new"
        self.state["intents"][CID]["payload"]["limit_price"] = price

    def test_owned_standard_contract_passes_readonly_without_claiming_stop_coverage(self):
        self.compare()
        self.assertEqual(self.result["unprotected_positions"], 0)
        self.assertEqual(self.result["fill_activity_versions"], 1)
        self.assertFalse(self.result["continuous_protection_proven"])
        self.assertFalse(self.result["broker_writes_performed"])

    def test_default_stock_only_config_cannot_accept_options(self):
        self.config.pop("options")
        self.failure()

    def test_disabled_options_policy_still_recognizes_existing_owned_position(self):
        self.config["options"]["enabled"] = False
        self.compare()

    def test_disabled_options_policy_cannot_approve_pending_new_entry(self):
        self.config["options"]["enabled"] = False
        self.pending()
        self.failure("risk_or_account_blocks_enable")
        self.assertIn("options_entries_disabled", self.result["risk_check_failures"])

    def test_unknown_option_position_never_adopted_even_with_valid_contract(self):
        self.state["owners"] = {}
        self.failure("unknown_activity")

    def test_known_open_order_without_verified_owner_lineage_is_rejected(self):
        stranger = dict(self.broker.parent, id="synthetic-stranger", status="new", filled_qty="0")
        self.broker.open_values = [stranger]
        self.state["known_order_ids"].append(stranger["id"])
        self.failure("unknown_activity")

    def test_nonexpiry_owned_position_does_not_require_dne(self):
        for status in (None, "submitting", "uncertain", "rejected"):
            with self.subTest(status=status):
                self.state["owners"][SYMBOL]["dne_status"] = status
                self.result = reconcile._result()
                self.compare()
                self.assertEqual(self.result["unprotected_positions"], 0)
                self.assertFalse(self.result["continuous_protection_proven"])

    def test_overnight_option_residual_fails_even_with_dne_acknowledgement(self):
        for status in (None, "acknowledged"):
            with self.subTest(status=status):
                self.state["owners"][SYMBOL].update(entry_day="2026-10-14", dne_status=status)
                self.result = reconcile._result()
                self.failure("protective_coverage_missing")
                self.assertEqual(self.result["unprotected_positions"], 1)

    def test_missing_entry_day_cannot_approve_option_inventory(self):
        self.state["owners"][SYMBOL].pop("entry_day")
        self.failure("protective_coverage_missing")

    def test_option_residual_within_two_days_of_expiry_requires_exit(self):
        for day in (28, 29):
            for status in (None, "acknowledged"):
                with self.subTest(day=day, status=status):
                    self.broker.observed_at = datetime(2026, 10, day, 17, tzinfo=timezone.utc)
                    self.state["owners"][SYMBOL].update(entry_day=f"2026-10-{day}", dne_status=status)
                    self.result = reconcile._result()
                    self.failure("protective_coverage_missing")
                    self.assertEqual(self.result["unprotected_positions"], 1)

    def test_same_day_inventory_three_days_before_expiry_does_not_need_dne(self):
        self.broker.observed_at = datetime(2026, 10, 27, 17, tzinfo=timezone.utc)
        self.state["owners"][SYMBOL]["entry_day"] = "2026-10-27"
        self.compare()
        self.assertEqual(self.result["unprotected_positions"], 0)

    def test_expiry_day_residual_fails_with_missing_or_acknowledged_dne(self):
        self.broker.observed_at = datetime(2026, 10, 30, 17, tzinfo=timezone.utc)
        for status in (None, "submitting", "uncertain", "rejected", "acknowledged"):
            with self.subTest(status=status):
                self.state["owners"][SYMBOL].update(entry_day="2026-10-30", dne_status=status)
                self.result = reconcile._result()
                self.failure("protective_coverage_missing")
                self.assertEqual(self.result["unprotected_positions"], 1)

    def test_option_session_day_uses_new_york_not_utc(self):
        # UTC has rolled to October 16; the New York date is still October 15.
        self.broker.observed_at = datetime(2026, 10, 16, 0, 1, tzinfo=timezone.utc)
        self.broker.is_open = False
        self.failure("protective_coverage_missing")

    def test_option_inventory_at_session_exit_deadline_fails_readonly_check(self):
        for minute in (40, 41, 59):
            with self.subTest(minute=minute):
                self.broker.observed_at = datetime(2026, 10, 15, 19, minute, tzinfo=timezone.utc)
                self.result = reconcile._result()
                self.failure("protective_coverage_missing")
                self.assertEqual(self.result["unprotected_positions"], 1)

    def test_option_inventory_before_session_exit_deadline_can_pass(self):
        self.broker.observed_at = datetime(2026, 10, 15, 19, 39, 59, tzinfo=timezone.utc)
        self.compare()

    def test_shortened_session_uses_broker_close_instead_of_sixteen_hundred(self):
        self.broker.observed_at = datetime(2026, 10, 15, 16, 40, tzinfo=timezone.utc)
        self.broker.close_override = "2026-10-15T17:00:00Z"
        self.failure("protective_coverage_missing")

    def test_closed_session_option_residual_fails_with_future_next_close(self):
        self.broker.is_open = False
        self.broker.close_override = "2026-10-16T20:00:00Z"
        self.failure("protective_coverage_missing")

    def test_open_session_cannot_guess_missing_naive_invalid_or_future_day_close(self):
        for close in (None, "2026-10-15T20:00:00", "not-a-time", "2026-10-16T20:00:00Z", "2026-10-15T16:00:00Z"):
            with self.subTest(close=close):
                self.broker.clock = lambda: {"timestamp": NOW.isoformat(), "is_open": True, "next_close": close}
                self.result = reconcile._result()
                self.failure("broker_response_invalid")

    def test_expired_contract_is_not_reclassified_as_safe_owned_inventory(self):
        self.broker.observed_at = datetime(2026, 11, 2, 17, tzinfo=timezone.utc)
        self.state["owners"][SYMBOL].update(entry_day="2026-11-02", dne_status="acknowledged")
        self.failure("order_identity_mismatch")

    def test_contract_expiry_strike_and_underlying_must_match_occ_and_intent(self):
        for field, value in (("expiration_date", "2026-11-13"), ("strike_price", "651"), ("underlying_symbol", "QQQ"), ("size", 10), ("type", "put")):
            with self.subTest(field=field):
                self.setUp()
                self.state["owners"][SYMBOL]["contract"][field] = value
                self.state["intents"][CID]["contract"][field] = value
                self.failure()

    def test_changed_entry_contract_metadata_is_not_accepted_as_owner_identity(self):
        self.state["intents"][CID]["contract"]["expiration_date"] = "2026-11-13"
        self.failure("order_identity_mismatch")

    def test_broker_expiry_mismatch_is_rejected(self):
        self.broker.position_values[0]["expiration_date"] = "2026-11-13"
        self.failure("order_identity_mismatch")

    def test_short_fractional_or_multi_contract_positions_refused(self):
        for qty in ("-1", "0.5", "2", "NaN"):
            with self.subTest(qty=qty):
                self.broker.position_values[0]["qty"] = qty
                self.failure("broker_response_invalid")

    def test_option_order_quantity_and_position_intent_are_bounded(self):
        self.broker.parent["qty"] = "2"
        self.state["intents"][CID]["payload"]["qty"] = "2"
        self.failure()
        self.setUp()
        self.broker.parent["position_intent"] = "sell_to_open"
        self.failure("order_identity_mismatch")

    def test_option_asset_class_or_multiplier_cannot_be_forged_as_equity(self):
        for field, value in (("asset_class", "us_equity"), ("multiplier", 1)):
            with self.subTest(field=field):
                self.setUp()
                self.broker.position_values[0][field] = value
                self.failure("broker_response_invalid")

    def test_adjusted_root_or_invalid_occ_date_is_rejected(self):
        for symbol in ("SPY1261106C00650000", "SPY261332C00650000", "SPY261106C00000000"):
            with self.subTest(symbol=symbol):
                row = dict(self.broker.parent, symbol=symbol)
                with self.assertRaises(reconcile.ReconciliationFailure):
                    reconcile._order_map([row])

    def test_pending_option_premium_is_multiplied_by_100_and_fee_reserved(self):
        self.pending("9.95")
        self.failure("risk_or_account_blocks_enable")
        self.assertIn("acquisition_sleeve_limit", self.result["risk_check_failures"])
        self.assertIn("pending_buy_sleeve_limit", self.result["risk_check_failures"])

    def test_pending_premium_and_fee_obey_cash_reserve(self):
        self.pending("2.00")
        self.broker.account_value["cash"] = "2205"
        self.failure("risk_or_account_blocks_enable")
        self.assertIn("cash_reserve_limit", self.result["risk_check_failures"])

    def test_existing_option_basis_includes_fee_contingency(self):
        self.broker.position_values[0].update(cost_basis="995", market_value="995")
        self.failure("risk_or_account_blocks_enable")
        self.assertIn("acquisition_sleeve_limit", self.result["risk_check_failures"])

    def test_option_appreciation_does_not_prevent_exit_management(self):
        self.broker.position_values[0]["market_value"] = "1500"
        self.compare()
        self.assertTrue(self.result["valuation_above_entry_budget"])

    def test_options_allocation_must_fit_the_same_total_capital(self):
        self.config["intraday"]["allocation"] = 50000
        self.failure("risk_or_account_blocks_enable")

    def test_options_allocation_cannot_exceed_authorized_thousand(self):
        self.config["options"]["allocation"] = 1001
        self.config["intraday"]["allocation"] = 48999
        self.failure("risk_or_account_blocks_enable")

    def test_option_fill_unknown_order_or_wrong_contract_remains_unknown(self):
        self.broker.activities[0]["symbol"] = "SPY261113C00650000"
        self.failure("unknown_activity")

    def test_option_fill_fraction_or_multiple_contracts_not_accepted(self):
        for quantity in (".5", "2"):
            with self.subTest(quantity=quantity):
                self.broker.activities[0]["qty"] = quantity
                self.failure("broker_response_invalid")

    def test_inventory_asset_class_changed_during_read_is_not_accepted(self):
        self.broker.mutate_second_read = True
        self.failure()

    def test_uncertain_option_order_not_found_is_unresolved_never_resubmitted(self):
        self.state["intents"][CID]["status"] = "uncertain"
        self.state["intents"][CID].pop("order_id")
        self.broker.parent = None
        self.failure("unresolved_intents")


if __name__ == "__main__":
    unittest.main()
