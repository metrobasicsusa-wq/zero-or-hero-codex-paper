import copy
import math
import unittest
from datetime import date, timedelta

from zerotrader.swing import DEFAULT_PARAMS, _Market, _backtest, rank, research


def sessions(count):
    value = date(2023, 1, 2)
    output = []
    while len(output) < count:
        if value.weekday() < 5:
            output.append(value.isoformat())
        value += timedelta(days=1)
    return output


def row(day, close, opening=None, low=None):
    opening = close if opening is None else opening
    return {"t": day + "T00:00:00Z", "o": opening, "h": max(opening, close) * 1.003,
            "l": min(opening, close) * 0.995 if low is None else low,
            "c": close, "v": 1_000_000}


def market_data(count=430):
    days = sessions(count)
    output = {"SPY": [row(day, 400 * math.exp(i * 0.0005)) for i, day in enumerate(days)]}
    for symbol, slope in (("AAA", 0.0013), ("BBB", 0.0010), ("CCC", 0.0008), ("DDD", 0.0006)):
        output[symbol] = [row(day, 30 * math.exp(i * slope + 0.005 * math.sin(i / 6))) for i, day in enumerate(days)]
    return output


class SwingTests(unittest.TestCase):
    def test_rank_prefix_invariance_and_asof_excludes_future(self):
        data = market_data(430)
        cutoff = data["SPY"][270]["t"][:10]
        prefix = {symbol: rows[:271] for symbol, rows in data.items()}
        expected = rank(prefix, DEFAULT_PARAMS)
        altered = copy.deepcopy(data)
        for symbol, rows in altered.items():
            for i in range(271, len(rows)):
                rows[i] = row(rows[i]["t"][:10], 10_000 if symbol == "BBB" else 1)
        self.assertTrue(expected)
        self.assertEqual(expected, rank(data, DEFAULT_PARAMS, cutoff))
        self.assertEqual(expected, rank(altered, DEFAULT_PARAMS, cutoff))
        self.assertNotIn("SPY", [item["symbol"] for item in expected])

    def test_market_trend_can_hold_cash(self):
        data = market_data(200)
        for i, item in enumerate(data["SPY"]):
            data["SPY"][i] = row(item["t"][:10], 500 - i)
        self.assertEqual([], rank(data, DEFAULT_PARAMS))
        self.assertEqual([], rank({"AAA": data["AAA"]}, DEFAULT_PARAMS))

    def test_invalid_history_and_stale_symbol_are_excluded(self):
        data = market_data(200)
        data["BBB"].pop(-2)
        data["CCC"][-1]["c"] = float("nan")
        data["DDD"][-1] = row(data["DDD"][-1]["t"][:10], 5)
        self.assertEqual(["AAA"], [item["symbol"] for item in rank(data, DEFAULT_PARAMS)])

    def test_risk_parameters_cannot_be_evolved(self):
        for value in ({"stop_pct": 0.06}, {"max_drawdown": 0.2}, {"momentum": 59}, {"top_n": True}):
            with self.assertRaises(ValueError):
                rank({}, value)

    def test_backtest_waits_for_next_open_and_charges_both_sides(self):
        data = market_data(140)
        days = sessions(140)
        data = {"SPY": data["SPY"], "AAA": [row(day, 20) for day in days]}
        data["AAA"][130] = row(days[130], 30, opening=20, low=20)
        data["AAA"][131] = row(days[131], 25, opening=25, low=25)
        market = _Market(data)
        before_signal = _backtest(market, DEFAULT_PARAMS, 130, 131)
        after_signal = _backtest(market, DEFAULT_PARAMS, 131, 132)
        self.assertEqual(0, before_signal["fills"])
        self.assertEqual(1, after_signal["fills"])
        # Whole shares: floor(50000 / 5 / 1.001 / 25) = 399.
        self.assertAlmostEqual(50_000 - 399 * 25 * 0.002, after_signal["final_equity"], places=2)
        self.assertLessEqual(after_signal["max_gross_exposure"], 1)

    def test_backtest_prefix_invariance(self):
        data = market_data(430)
        expected = _backtest(_Market(data), DEFAULT_PARAMS, 121, 270)
        prefix = {symbol: rows[:270] for symbol, rows in data.items()}
        self.assertEqual(expected, _backtest(_Market(prefix), DEFAULT_PARAMS, 121, 270))

    def test_retained_position_keeps_shares_without_daily_resize(self):
        data = market_data(210)
        data = {"SPY": data["SPY"], "AAA": data["AAA"]}
        result = _backtest(_Market(data), DEFAULT_PARAMS, 121, 200)
        entry = data["AAA"][121]["o"]
        final_close = data["AAA"][199]["c"]
        quantity = math.floor((50_000 / 5) / (entry * 1.001))
        expected_equity = 50_000 - quantity * entry * 1.001 + quantity * final_close * 0.999
        self.assertEqual(1, result["fills"])
        self.assertEqual(0, result["stops"])
        self.assertAlmostEqual(expected_equity, result["final_equity"], places=2)

    def test_overnight_gap_stop_fills_at_gap_open(self):
        days = sessions(140)
        data = market_data(140)
        data = {"SPY": data["SPY"], "AAA": [row(day, 10 + i / 10) for i, day in enumerate(days)]}
        data["AAA"][121] = row(days[121], 30, opening=30, low=30)
        data["AAA"][122] = row(days[122], 20, opening=20, low=20)
        result = _backtest(_Market(data), DEFAULT_PARAMS, 121, 123)
        quantity = math.floor(50_000 / 5 / 1.001 / 30)
        expected_equity = 50_000 - quantity * 30 * 1.001 + quantity * 20 * 0.999
        self.assertEqual(1, result["stops"])
        self.assertEqual(2, result["fills"])  # no immediate repurchase after gap stop
        self.assertAlmostEqual(expected_equity, result["final_equity"], places=2)

    def test_intraday_stop_is_triggered_after_entry(self):
        data = market_data(140)
        data = {"SPY": data["SPY"], "AAA": data["AAA"]}
        day = data["AAA"][121]["t"][:10]
        data["AAA"][121] = row(day, 30, opening=30, low=28)
        result = _backtest(_Market(data), DEFAULT_PARAMS, 121, 122)
        quantity = math.floor(50_000 / 5 / 1.001 / 30)
        expected_equity = 50_000 - quantity * 30 * 1.001 + quantity * 28.5 * 0.999
        self.assertEqual(1, result["stops"])
        self.assertAlmostEqual(expected_equity, result["final_equity"], places=2)

    def test_insufficient_research_keeps_incumbent(self):
        incumbent = {"momentum": 20, "trend": 120, "top_n": 3, "stop_pct": 0.05}
        result = research(market_data(300), incumbent)
        self.assertFalse(result["recommend_promote"])
        self.assertEqual(incumbent, result["params"])
        self.assertEqual(300, result["validation"]["available_sessions"])

    def test_holdout_cannot_change_nomination(self):
        data = market_data(430)
        original = research(data)
        boundary = original["validation"]["segments"]["holdout"]["start"]
        altered = copy.deepcopy(data)
        for symbol, rows in altered.items():
            for i, item in enumerate(rows):
                if item["t"][:10] >= boundary:
                    rows[i] = row(item["t"][:10], (200 if symbol == "SPY" else 2) * math.exp(-i / 1000))
        changed = research(altered)
        self.assertEqual(original["validation"]["nominated_name"], changed["validation"]["nominated_name"])
        self.assertEqual(original["metrics"]["candidate"]["training"], changed["metrics"]["candidate"]["training"])
        self.assertEqual(original["metrics"]["candidate"]["validation"], changed["metrics"]["candidate"]["validation"])
        self.assertFalse(changed["recommend_promote"])
        self.assertEqual(DEFAULT_PARAMS, changed["params"])
        self.assertEqual(0.05, original["params"]["stop_pct"])
        self.assertLessEqual(original["metrics"]["candidate"]["holdout"]["max_gross_exposure"], 1)
        segments = original["validation"]["segments"]
        self.assertLess(segments["training"]["end"], segments["validation"]["start"])
        self.assertLess(segments["validation"]["end"], segments["holdout"]["start"])


if __name__ == "__main__":
    unittest.main()
