import copy
from datetime import date, datetime, timedelta
import json
import unittest

from zerotrader import intraday, swing
from zerotrader.samples import build_samples


ASOF = '2026-09-30T10:15:00-04:00'


def daily_market(symbols, count=150):
    days = []
    day = date(2026, 9, 29)
    while len(days) < count:
        if day.weekday() < 5:
            days.append(day)
        day -= timedelta(days=1)
    days.reverse()
    return {symbol: [dict(t=day.isoformat() + 'T04:00:00Z', o=100 + i, h=101 + i,
                         l=99 + i, c=100 + i, v=1000)
                     for i, day in enumerate(days)] for symbol in ['SPY'] + list(symbols)}


def intraday_session(day='2026-09-30', volume=200):
    start = datetime.fromisoformat(day + 'T09:30:00-04:00')
    return [dict(t=start.isoformat(), o=100, h=100.5, l=99.5, c=100, v=100),
            dict(t=(start + timedelta(minutes=15)).isoformat(), o=100, h=100.8, l=99.8, c=100.2, v=100),
            dict(t=(start + timedelta(minutes=30)).isoformat(), o=100.3, h=102, l=100.2, c=101.8, v=volume)]


def config(swing_symbols=(), intra_symbols=()):
    return {'swing_universe': list(swing_symbols), 'intraday_universe': list(intra_symbols)}


def signals(daily, minute, asof=ASOF, swing_params=None, intra_params=None):
    return {'swing': swing.rank(daily, swing_params),
            'intraday': intraday.rank(minute, intra_params or intraday.DEFAULT_PARAMS, asof)}


class SampleTests(unittest.TestCase):
    def test_entire_universe_including_missing_and_unselected_is_retained(self):
        names = ['S%02d' % i for i in range(50)]
        daily = daily_market(names[:8])
        minute = {symbol: intraday_session() for symbol in names[:4]}
        supplied = signals(daily, minute)
        result = build_samples(daily, minute, config(names, names[:10]), {}, supplied, ASOF)
        self.assertEqual(len(result), 60)
        self.assertEqual(len({(row['sleeve'], row['symbol']) for row in result}), 60)
        for sleeve in ('swing', 'intraday'):
            self.assertEqual({row['symbol'] for row in result if row['sleeve'] == sleeve and row['decision'] == 'selected'},
                             {row['symbol'] for row in supplied[sleeve]})
        self.assertTrue(any(row['decision'] == 'data_unavailable' for row in result))
        self.assertTrue(any(row['decision'] == 'not_selected' for row in result))

    def test_eligible_losers_have_rank_and_below_top_n(self):
        names = ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF', 'GGG']
        daily = daily_market(names)
        minute = {symbol: intraday_session() for symbol in names[:4]}
        result = build_samples(daily, minute, config(names, names[:4]), {}, signals(daily, minute), ASOF)
        losers = [row for row in result if row['decision'] == 'not_selected']
        self.assertEqual([(row['sleeve'], row['symbol'], row['rank']) for row in losers],
                         [('swing', 'FFF', 6), ('swing', 'GGG', 7), ('intraday', 'DDD', 4)])
        self.assertTrue(all(row['reason_codes'] == ['below_top_n'] for row in losers))

    def test_supplied_selection_is_authoritative_and_omitted_sleeve_is_not_evaluated(self):
        daily = daily_market(['AAA', 'BBB'])
        minute = {'AAA': intraday_session()}
        result = build_samples(daily, minute, config(['AAA', 'BBB'], ['AAA']), {},
                               {'swing': [{'symbol': 'BBB'}]}, ASOF)
        self.assertEqual(result[0]['decision'], 'not_selected')
        self.assertEqual(result[0]['reason_codes'], ['did_not_meet_signal'])
        self.assertEqual(result[1]['decision'], 'selected')
        self.assertEqual(result[1]['rank'], 1)
        self.assertEqual(result[2]['decision'], 'not_evaluated')
        self.assertEqual(result[2]['reason_codes'], ['sleeve_not_evaluated'])
        self.assertIsNone(result[2]['rank'])
        self.assertEqual(result[2]['features']['price'], 101.8)

    def test_future_poison_and_incomplete_daily_or_intraday_do_not_change_rows(self):
        daily = daily_market(['AAA'])
        minute = {'AAA': intraday_session()}
        supplied = signals(daily, minute)
        cfg = config(['AAA'], ['AAA'])
        before = build_samples(daily, minute, cfg, {}, supplied, ASOF)
        poisoned_daily = copy.deepcopy(daily)
        for values in poisoned_daily.values():
            for day in ('2026-09-30', '2026-10-01'):
                values.append(dict(t=day + 'T04:00:00Z', o=100000, h=100001, l=99999, c=100000, v=99999999))
        poisoned_minute = copy.deepcopy(minute)
        poisoned_minute['AAA'].append(dict(t='2026-09-30T10:15:00-04:00', o=100000,
                                           h=100001, l=99999, c=100000, v=99999999))
        poisoned_minute['AAA'].extend(intraday_session('2026-10-01'))
        self.assertEqual(before, build_samples(poisoned_daily, poisoned_minute, cfg, {}, supplied, ASOF))
        self.assertEqual(before[0]['bar_time'], '2026-09-29')
        self.assertEqual(before[1]['features']['signal_time'], ASOF)
        self.assertNotIn('future_return', before[0]['features'])

    def test_invalid_rows_are_excluded_and_archive_is_strict_json(self):
        daily = daily_market(['AAA'])
        minute = {'AAA': intraday_session()}
        supplied = signals(daily, minute)
        cfg = config(['AAA'], ['AAA'])
        before = build_samples(daily, minute, cfg, {}, supplied, ASOF)
        daily['AAA'].extend([dict(daily['AAA'][-1], c=float('nan')), {'t': 'bad'},
                             dict(daily['AAA'][-1], l=10000), dict(daily['AAA'][-1], c=float('inf'))])
        minute['AAA'].extend([dict(minute['AAA'][-1], c=float('nan')), {'t': 'bad'},
                              dict(minute['AAA'][-1], t='2026-09-30T10:01:00-04:00'),
                              dict(minute['AAA'][-1], v=10 ** 1000)])
        after = build_samples(daily, minute, cfg, {}, supplied, ASOF)
        self.assertEqual(before, after)
        json.dumps(after, allow_nan=False)

    def test_swing_active_parameters_and_features_match_strategy(self):
        p = dict(swing.DEFAULT_PARAMS, momentum=20, trend=120, top_n=3)
        daily = daily_market(['AAA'])
        state = {'strategies': {'swing': {'params': p, 'stage': 'validated'}}}
        chosen = swing.rank(daily, p)
        result = build_samples(daily, {}, config(['AAA']), state, {'swing': chosen}, ASOF)[0]
        self.assertEqual(result['params'], p)
        self.assertEqual(result['score'], chosen[0]['score'])
        self.assertEqual(result['features']['price'], chosen[0]['price'])
        self.assertAlmostEqual(result['features']['momentum'], 249 / 229 - 1)
        self.assertAlmostEqual(result['features']['trend_sma'], sum(range(130, 250)) / 120)
        self.assertTrue(result['features']['sessions_aligned_21'])

    def test_swing_missing_and_gappy_history_are_not_trade_rejections(self):
        daily = daily_market(['STALE', 'GAPPY', 'SHORT'])
        daily['STALE'].pop()
        daily['GAPPY'].pop(-10)
        daily['SHORT'] = daily['SHORT'][-10:]
        result = build_samples(daily, {}, config(['STALE', 'GAPPY', 'SHORT', 'MISSING']), {},
                               {'swing': []}, ASOF)
        for row, reason in zip(result, ('benchmark_session_missing', 'daily_session_gap',
                                       'daily_history_insufficient', 'daily_data_missing')):
            self.assertEqual(row['decision'], 'data_unavailable')
            self.assertIn(reason, row['reason_codes'])

    def test_intraday_after_hours_is_not_a_data_outage(self):
        result = build_samples({}, {'AAA': intraday_session()}, config([], ['AAA', 'MISSING']), {},
                               {'intraday': []}, '2026-09-30T18:00:00-04:00')
        for row in result:
            self.assertEqual(row['decision'], 'not_selected')
            self.assertIn('outside_entry_window', row['reason_codes'])

    def test_intraday_stale_and_missing_opening_range(self):
        minute = {'STALE': intraday_session()[:2], 'GAPPY': intraday_session()[1:]}
        result = build_samples({}, minute, config([], ['STALE', 'GAPPY']), {}, {'intraday': []}, ASOF)
        self.assertEqual(result[0]['decision'], 'data_unavailable')
        self.assertIn('intraday_signal_stale', result[0]['reason_codes'])
        self.assertEqual(result[1]['decision'], 'data_unavailable')
        self.assertIn('opening_range_incomplete', result[1]['reason_codes'])

    def test_intraday_same_slot_uses_only_last_twenty_prior_observations(self):
        start = date(2026, 9, 1)
        historical = []
        for i in range(25):
            historical.extend(intraday_session((start + timedelta(days=i)).isoformat(), volume=100 if i >= 5 else 100000))
        minute = {'AAA': historical + intraday_session(volume=200)}
        chosen = intraday.rank(minute, intraday.DEFAULT_PARAMS, ASOF)
        row = build_samples({}, minute, config([], ['AAA']), {}, {'intraday': chosen}, ASOF)[0]
        features = row['features']
        self.assertEqual(features['same_slot_prior_observations'], 20)
        self.assertEqual(features['same_slot_prior_mean_volume'], 100)
        self.assertEqual(features['volume_baseline_source'], 'prior_same_slot')
        self.assertEqual(features['volume_ratio'], 2)
        self.assertEqual(row['score'], chosen[0]['score'])
        self.assertAlmostEqual(features['session_vwap'], chosen[0]['vwap'], places=7)

    def test_intraday_fallback_volume_and_sixty_minute_range(self):
        p = dict(intraday.DEFAULT_PARAMS, opening_range_minutes=60)
        minute = {'AAA': intraday_session()}
        row = build_samples({}, minute, config([], ['AAA']), {'intraday': {'params': p}},
                            {'intraday': []}, ASOF)[0]
        self.assertEqual(row['decision'], 'not_selected')
        self.assertIn('opening_range_not_ready', row['reason_codes'])
        self.assertEqual(row['features']['volume_baseline_source'], 'current_session_prior_bars')
        self.assertEqual(row['features']['volume_ratio'], 2)

    def test_requires_explicit_timezone_for_asof(self):
        with self.assertRaises(ValueError):
            build_samples({}, {}, config(), {}, {}, '2026-09-30T10:15:00')


if __name__ == '__main__':
    unittest.main()
