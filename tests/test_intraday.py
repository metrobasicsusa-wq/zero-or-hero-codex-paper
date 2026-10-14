import unittest
from datetime import datetime, timedelta

from zerotrader import intraday


def bar(stamp, o, h, l, c, v=100):
    return dict(t=stamp.isoformat(), o=o, h=h, l=l, c=c, v=v)


def session(day="2026-09-01", stop=False, gap=False, early=False):
    start = datetime.fromisoformat(day + "T09:30:00-04:00")
    result = [bar(start, 100, 100.5, 99.5, 100),
              bar(start + timedelta(minutes=15), 100, 100.8, 99.8, 100.2),
              bar(start + timedelta(minutes=30), 100.3, 102, 100.2, 101.8, 200)]
    for i in range(3, 14 if early else 26):
        result.append(bar(start + timedelta(minutes=15 * i), 102, 102.5, 101.8, 102.2))
    if stop:
        result[4] = bar(start + timedelta(minutes=60), 102, 102.2, 99, 100)
    if gap:
        result[4] = bar(start + timedelta(minutes=60), 98, 98.5, 97, 98)
    return result


class IntradayTests(unittest.TestCase):
    def test_only_completed_bars_and_opening_range(self):
        rows = session()
        self.assertEqual(intraday.rank({"AAA": rows}, intraday.DEFAULT_PARAMS, "2026-09-01T09:59:00-04:00"), [])
        self.assertEqual(intraday.rank({"AAA": rows}, intraday.DEFAULT_PARAMS, "2026-09-01T10:14:59-04:00"), [])
        ranked = intraday.rank({"AAA": rows}, intraday.DEFAULT_PARAMS, "2026-09-01T10:15:00-04:00")
        self.assertEqual([r["symbol"] for r in ranked], ["AAA"])
        self.assertEqual(ranked[0]["price"], 101.8)
        self.assertEqual(ranked[0]["volume_ratio"], 2)

    def test_future_bars_cannot_change_signal(self):
        rows = session()
        early = intraday.rank({"AAA": rows[:3]}, intraday.DEFAULT_PARAMS, "2026-09-01T10:15:00-04:00")
        rows[-1]["h"] = rows[-1]["c"] = 1000
        rows[-1]["v"] = 1000000
        with_future = intraday.rank({"AAA": rows}, intraday.DEFAULT_PARAMS, "2026-09-01T10:15:00-04:00")
        self.assertEqual(early, with_future)

    def test_sixty_minute_opening_range_not_ready(self):
        p = dict(intraday.DEFAULT_PARAMS, opening_range_minutes=60)
        self.assertEqual(intraday.rank({"AAA": session()}, p, "2026-09-01T10:15:00-04:00"), [])

    def test_missing_opening_bar_no_signal(self):
        self.assertEqual(intraday.rank({"AAA": session()[1:]}, intraday.DEFAULT_PARAMS, "2026-09-01T10:15:00-04:00"), [])

    def test_no_stale_or_late_entry(self):
        self.assertEqual(intraday.rank({"AAA": session()[:3]}, intraday.DEFAULT_PARAMS, "2026-09-01T10:30:00-04:00"), [])
        self.assertEqual(intraday.rank({"AAA": session()}, intraday.DEFAULT_PARAMS, "2026-09-01T14:31:00-04:00"), [])

    def test_max_three_candidates(self):
        ranked = intraday.rank({s: session() for s in ("AAA", "BBB", "CCC", "DDD")},
                               intraday.DEFAULT_PARAMS, "2026-09-01T10:15:00-04:00")
        self.assertEqual(len(ranked), 3)

    def test_next_open_fill_and_same_day_close(self):
        trades = intraday._simulate(intraday._prepare({"AAA": session()}), intraday.DEFAULT_PARAMS)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["entry_price"], 102)
        self.assertEqual(trades[0]["entry_time"], "2026-09-01T10:15:00-04:00")
        self.assertEqual(trades[0]["exit_time"], "2026-09-01T15:45:00-04:00")
        self.assertLess(trades[0]["net_return"], 102.2 / 102 - 1)

    def test_early_close(self):
        trades = intraday._simulate(intraday._prepare({"AAA": session(early=True)}), intraday.DEFAULT_PARAMS)
        self.assertEqual(trades[0]["exit_time"], "2026-09-01T13:00:00-04:00")

    def test_stop_and_gap_fill(self):
        for mode, price, reason in [(dict(stop=True), 102 * .985, "stop"),
                                    (dict(gap=True), 98, "stop_gap")]:
            trades = intraday._simulate(intraday._prepare({"AAA": session(**mode)}), intraday.DEFAULT_PARAMS)
            self.assertEqual(trades[0]["exit_price"], price)
            self.assertEqual(trades[0]["exit_reason"], reason)

    def test_missing_next_bar_does_not_fill(self):
        rows = session()[:3] + session()[4:]
        trades = intraday._simulate(intraday._prepare({"AAA": rows}), intraday.DEFAULT_PARAMS)
        self.assertEqual(trades, [])

    def test_insufficient_research_does_not_promote(self):
        result = intraday.research({"AAA": session()})
        self.assertFalse(result["recommend_promote"])
        self.assertEqual(result["params"], intraday.DEFAULT_PARAMS)
        self.assertEqual(result["validation"]["sessions"], 1)

    def test_chronological_holdout_and_trade_gate(self):
        rows = []
        start = datetime(2026, 5, 1)
        for i in range(60):
            rows.extend(session((start + timedelta(days=i)).date().isoformat()))
        result = intraday.research({"AAA": rows})
        split = result["validation"]["split_dates"]
        self.assertLess(split["train"]["end"], split["validation"]["start"])
        self.assertLess(split["validation"]["end"], split["holdout"]["start"])
        self.assertEqual(result["metrics"]["holdout"]["sessions"], 12)
        self.assertFalse(result["recommend_promote"])
        self.assertFalse(result["validation"]["selection_uses_holdout"])
        self.assertTrue(all("holdout" not in r for r in result["rankings"]))

    def test_fixed_risk_and_bounded_parameters(self):
        for p in ({"stop_pct": .5}, {"top_n": 20}, {"opening_range_minutes": 5}):
            with self.assertRaises(ValueError):
                intraday.rank({}, p)

    def test_holdout_only_confirms_and_never_selects(self):
        rows = []
        start = datetime(2026, 5, 1)
        for i in range(60):
            day_rows = session((start + timedelta(days=i)).date().isoformat())
            for row in day_rows[3:]:
                row.update(h=105.2, c=105)
            rows.extend(day_rows)
        positive = intraday.research({"AAA": rows, "BBB": rows})
        self.assertTrue(positive["recommend_promote"])
        self.assertGreaterEqual(positive["metrics"]["holdout"]["trades"], 20)
        damaged = [dict(row) for row in rows]
        for i in range(48, 60):
            damaged[i * 26 + 4].update(o=98, h=98.5, l=97, c=98)
        negative = intraday.research({"AAA": damaged, "BBB": damaged})
        self.assertEqual(positive["selected_name"], negative["selected_name"])
        self.assertEqual(positive["rankings"], negative["rankings"])
        self.assertFalse(negative["recommend_promote"])


if __name__ == "__main__":
    unittest.main()
