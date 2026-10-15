"""Offline adversarial policy/accounting tests; no broker or credentials."""
import copy
from datetime import datetime, timedelta
import json
import unittest
from zoneinfo import ZoneInfo

from zerotrader.options_pilot import (parse_option_symbol, policy_config,
    reservation_summary, select_candidates, validate_contract, validated_option_quote)


NOW = datetime(2026, 10, 15, 11, 0, tzinfo=ZoneInfo("America/New_York"))
SYMBOL = "AAPL261023C00100000"


def config():
    return {"mode": "paper", "capital": 100000, "max_drawdown": .10,
            "swing": {"allocation": 50000}, "intraday": {"allocation": 49000},
            "intraday_universe": ["AAPL"], "max_quote_age_seconds": 90,
            "max_spread_fraction": .005, "options": {"enabled": True}}


def contract(**changes):
    result = {"symbol": SYMBOL, "underlying_symbol": "AAPL", "type": "call",
              "expiration_date": "2026-10-23", "strike_price": "100", "size": "100",
              "status": "active", "tradable": True}
    result.update(changes)
    return result


def quote(**changes):
    result = {"bp": "2.20", "ap": "2.30", "t": NOW.isoformat(), "feed": "indicative"}
    result.update(changes)
    return result


def state(count=1, status="new", price="2.40"):
    return {"strategies": {}, "owners": {}, "fills": {}, "intents": {
        f"c{i}": {"sleeve": "options", "order_id": f"o{i}", "status": status,
                  "payload": {"symbol": SYMBOL, "side": "buy", "type": "limit",
                              "qty": "1", "position_intent": "buy_to_open", "limit_price": price}}
        for i in range(count)}}


class OptionsPolicyTests(unittest.TestCase):
    def test_disabled_by_default_and_json_safe(self):
        self.assertFalse(policy_config({})["enabled"])
        json.dumps(policy_config(config()))

    def test_capital_is_carved_out_not_added(self):
        cfg = config()
        cfg["intraday"]["allocation"] = 50000
        with self.assertRaises(ValueError):
            policy_config(cfg)
        cfg["intraday"]["allocation"] = 49000
        self.assertTrue(policy_config(cfg)["enabled"])

    def test_unsafe_or_unknown_configuration_fails(self):
        for field, value in (("allocation",1001), ("lifetime_premium_cap",961),
                             ("max_lifetime_contracts",5), ("max_positions",3),
                             ("max_premium_per_contract",241), ("quote_feed","opra"),
                             ("max_spread_fraction",.11), ("max_quote_age_seconds",91),
                             ("min_dte",0), ("max_dte",22), ("enabled","true"),
                             ("allocation","NaN"), ("mystery",True)):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                cfg = config()
                cfg["options"][field] = value
                policy_config(cfg)

    def test_parse_and_contract_identity(self):
        self.assertEqual(parse_option_symbol(SYMBOL)["strike"], 100)
        self.assertEqual(validate_contract(contract(), NOW)["dte"], 8)
        for symbol in ("AAPL", "AAPL1261023C00100000", "AAPL261332C00100000", "AAPL261023C00000000"):
            with self.subTest(symbol=symbol), self.assertRaises(ValueError):
                parse_option_symbol(symbol)
        for changes in ({"size":10},{"tradable":False},{"status":"inactive"},
                        {"strike_price":101},{"type":"put"},{"expiration_date":"2026-10-24"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validate_contract(contract(**changes), NOW)

    def test_indicative_is_explicit_and_not_executable(self):
        checked = validated_option_quote(quote(), NOW)
        self.assertFalse(checked["executable"])
        self.assertEqual(checked["feed"], "indicative")
        self.assertTrue(checked["limitations"])
        json.dumps(checked)
        for changes in ({"feed":"opra"}, {"feed":None}, {"executable":True},
                        {"bp":"NaN"}, {"ap":"Infinity"}, {"bp":0}, {"bp":3},
                        {"bp":1,"ap":2}, {"t":(NOW-timedelta(seconds=91)).isoformat()},
                        {"t":(NOW+timedelta(seconds=6)).isoformat()}, {"t":"2026-10-15T11:00:00"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                validated_option_quote(quote(**changes), NOW)


class ReservationTests(unittest.TestCase):
    def test_intent_and_cumulative_poll_are_not_double_counted(self):
        st = state(status="filled")
        st["fills"]["o0"] = {"symbol":SYMBOL,"sleeve":"options","side":"buy","qty":1,"price":2.20}
        result = reservation_summary(st, [], config())
        self.assertEqual(result["premium_reserved"], "240.00")
        self.assertEqual(result["contracts_reserved"], 1)
        self.assertEqual(result["fee_reserved"], "10.00")
        self.assertIn("not_actual_fill_statistics", result["accounting_source"])
        self.assertNotIn("actual_fill_count", result)
        json.dumps(result)

    def test_corrections_and_disappearing_intents_never_release_prior_reserve(self):
        st = state(status="filled", price="2.00")
        st["fills"]["o0"] = {"symbol":SYMBOL,"sleeve":"options","side":"buy","qty":1,"price":2.30}
        first = reservation_summary(st, [], config())
        st["fills"]["o0"]["price"] = 1
        second = reservation_summary(st, [], config(), first["risk_ledger"])
        self.assertEqual(second["premium_reserved"], "230.00")
        third = reservation_summary(state(0), [], config(), second["risk_ledger"])
        self.assertEqual(third["premium_reserved"], "230.00")

    def test_sale_proceeds_never_refuel_lifetime_allowance(self):
        st = state(4, status="filled")
        st["fills"]["sale"] = {"symbol":SYMBOL,"sleeve":"options","side":"sell","qty":4,"price":100}
        result = reservation_summary(st, [], config())
        self.assertEqual(result["total_reserved"], "1000.00")
        self.assertEqual(result["remaining_premium"], "0.00")
        self.assertEqual(result["remaining_contracts"], 0)
        self.assertFalse(result["can_enter"])
        self.assertFalse(result["sale_proceeds_reusable"])

    def test_pending_uncertain_and_cancelled_are_fully_reserved(self):
        for status in ("uncertain","submitting","canceled","rejected","expired"):
            with self.subTest(status=status):
                result = reservation_summary(state(status=status), [], config())
                self.assertEqual(result["premium_reserved"], "240.00")
        self.assertEqual(reservation_summary(state(2), [], config())["available_slots"], 0)

    def test_filled_before_position_arrives_keeps_slot_until_linked_close(self):
        st = state(2, status="filled")
        first = reservation_summary(st, [], config())
        self.assertEqual(first["active_contracts"], 2)
        self.assertEqual(first["available_slots"], 0)
        st["intents"]["exit0"] = {"sleeve":"options","order_id":"sold0","status":"filled",
            "exit_for":"c0", "payload":{"symbol":SYMBOL,"qty":"1","side":"sell",
                "position_intent":"sell_to_close","type":"market"}}
        st["fills"]["sold0"] = {"symbol":SYMBOL,"sleeve":"options","side":"sell","qty":1,"price":2.5}
        second = reservation_summary(st, [], config(), first["risk_ledger"])
        self.assertEqual(second["active_contracts"], 1)
        self.assertEqual(second["premium_reserved"], "480.00")
        st["fills"]["sold0"]["qty"] = 0
        st["fills"]["sold0"]["price"] = 0
        corrected = reservation_summary(st, [], config(), second["risk_ledger"])
        self.assertEqual(corrected["active_contracts"], 2)

    def test_only_never_submitted_failure_can_be_ignored(self):
        st = state(status="not_submitted")
        st["intents"]["c0"].pop("order_id")
        self.assertEqual(reservation_summary(st, [], config())["premium_reserved"], "0.00")
        prior = reservation_summary(state(), [], config())["risk_ledger"]
        self.assertEqual(reservation_summary(st, [], config(), prior)["premium_reserved"], "240.00")

    def test_unknown_option_fill_is_not_adopted(self):
        st = state(0)
        st["fills"]["unknown"] = {"symbol":SYMBOL,"sleeve":"options","side":"buy","qty":1,"price":1}
        with self.assertRaises(ValueError):
            reservation_summary(st, [], config())

    def test_unsafe_quantities_and_nonoptions_fail(self):
        for change in ({"qty":-1},{"qty":.5},{"qty":2},{"symbol":"AAPL"},
                       {"position_intent":"sell_to_open"},{"limit_price":"NaN"}):
            st = state()
            st["intents"]["c0"]["payload"].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                reservation_summary(st, [], config())

    def test_corrected_overspend_blocks_entries_and_preserves_evidence(self):
        st = state(status="filled")
        st["fills"]["o0"] = {"symbol":SYMBOL,"sleeve":"options","side":"buy","qty":1,"price":12}
        result = reservation_summary(st, [], config())
        self.assertEqual(result["premium_reserved"], "1200.00")
        self.assertFalse(result["can_enter"])
        self.assertIn("lifetime_budget_exceeded", result["entry_block_reasons"])

    def test_position_and_its_pending_entry_are_not_double_counted_but_second_order_is(self):
        st = state(2)
        st["owners"][SYMBOL] = {"sleeve":"options","entry_cid":"c0"}
        positions = [{"symbol":SYMBOL,"asset_class":"us_option","multiplier":100,
                      "qty":"1","side":"long","cost_basis":"230","market_value":"220"}]
        result = reservation_summary(st, positions, config())
        self.assertEqual(result["active_contracts"], 2)
        self.assertEqual(result["open_positions"], 1)
        for changes in ({"qty":-1},{"side":"short"},{"multiplier":10},{"cost_basis":None}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                reservation_summary(st, [dict(positions[0], **changes)], config())


class CandidateTests(unittest.TestCase):
    def select(self, *, signals=None, contracts=None, option_quote=None, stock_quote=None):
        signal = {"symbol":"AAPL","score":2,"reason":"completed stock breakout",
                  "signal_time":NOW.isoformat()}
        return select_candidates(signals if signals is not None else [signal],
                                 contracts if contracts is not None else [contract()],
                                 {SYMBOL:option_quote or quote()},
                                 {"AAPL":stock_quote or quote(bp=99.99,ap=100.01)}, NOW, config())

    def test_selects_one_contract_and_preserves_source_and_limitations(self):
        rows = self.select()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["qty"], 1)
        self.assertEqual(rows[0]["limit_price"], "2.30")
        self.assertEqual(rows[0]["premium"], "230.00")
        self.assertEqual(rows[0]["signal"]["reason"], "completed stock breakout")
        self.assertFalse(rows[0]["quote"]["executable"])
        json.dumps(rows)

    def test_no_future_stale_or_invented_stock_signal(self):
        for offset in (-900, 1):
            signals = [{"symbol":"AAPL","score":1,"reason":"test",
                        "signal_time":(NOW+timedelta(seconds=offset)).isoformat()}]
            self.assertEqual(self.select(signals=signals), [])
        self.assertEqual(self.select(signals=[]), [])

    def test_no_overprice_stale_underlying_or_wrong_contract(self):
        self.assertEqual(self.select(option_quote=quote(bp=2.4, ap=2.4001)), [])
        self.assertEqual(self.select(stock_quote=quote(bp=100,ap=100.01,t=(NOW-timedelta(seconds=100)).isoformat())), [])
        self.assertEqual(self.select(contracts=[contract(size=10)]), [])
        self.assertEqual(self.select(contracts=[contract(symbol="AAPL261023P00100000",type="put")]), [])
        self.assertEqual(self.select(contracts=[contract(symbol="AAPL261023C00102000",strike_price=102)]), [])


if __name__ == "__main__":
    unittest.main()
