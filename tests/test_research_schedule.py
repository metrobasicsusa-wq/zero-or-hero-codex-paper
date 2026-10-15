import copy
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zerotrader import runner
from zerotrader.engine import new_state

class ResearchScheduleTests(unittest.TestCase):
    def due(self, now, last=None, market_open=True):
        return runner.weekly_research_due(datetime.fromisoformat(now), last, market_open)

    def test_monday_first_date_boundary_and_duplicate(self):
        for at in ['2026-10-05T09:30:00-04:00','2026-10-10T10:00:00-04:00','2026-10-12T09:29:59-04:00','2026-10-13T10:00:00-04:00']:
            self.assertFalse(self.due(at))
        self.assertTrue(self.due('2026-10-12T09:30:00-04:00'))
        self.assertTrue(self.due('2026-10-12T10:00:00-04:00'))
        self.assertFalse(self.due('2026-10-12T10:00:00-04:00', '2026-10-12T13:31:00+00:00'))
        self.assertFalse(self.due('2026-10-12T10:00:00-04:00', market_open=False))

    def test_dst_and_next_week(self):
        self.assertFalse(self.due('2026-11-02T13:30:00+00:00'))
        self.assertTrue(self.due('2026-11-02T14:30:00+00:00'))
        self.assertTrue(self.due('2026-10-19T09:30:00-04:00','2026-10-12T09:30:00-04:00'))

    def test_deferred_candidates_preserve_parameters_until_close(self):
        state=new_state();runner.seed_strategies(state);before=copy.deepcopy(state['strategies'])
        result={'recommend_promote':True,'params':{'test':1},'reason':'test'}
        with tempfile.TemporaryDirectory() as d, patch.object(runner.swing,'research',return_value=copy.deepcopy(result)), patch.object(runner.intraday,'research',return_value=copy.deepcopy(result)):
            directory=Path(d);at=datetime.fromisoformat('2026-10-12T09:30:00-04:00')
            runner.learn(state,{}, {},directory,at,defer=True)
            self.assertEqual(state['strategies'],before)
            self.assertIsNone(runner.apply_pending_research(state,directory,at,True))
            self.assertIsNone(runner.apply_pending_research(state,directory,at,False))
            close=datetime.fromisoformat('2026-10-12T16:01:00-04:00')
            state['paused']=True
            self.assertIsNone(runner.apply_pending_research(state,directory,close,False))
            state['paused']=False
            applied=runner.apply_pending_research(state,directory,close,False)
            self.assertTrue(applied['swing']['promotion_applied'])
            self.assertNotIn('pending_research',state)
            self.assertIsNone(runner.apply_pending_research(state,directory,close,False))

    def test_stale_candidate_discarded(self):
        state=new_state();runner.seed_strategies(state);before=copy.deepcopy(state['strategies'])
        state['pending_research']={'evaluated_at':'2026-10-12T09:30:00-04:00','incumbent':before,'results':{}}
        with tempfile.TemporaryDirectory() as d:
            runner.apply_pending_research(state,Path(d),datetime.fromisoformat('2026-10-19T17:00:00-04:00'),False)
        self.assertEqual(state['strategies'],before)
        self.assertNotIn('pending_research',state)

    def test_options_accounting_does_not_invalidate_deferred_stock_research(self):
        state = new_state()
        runner.seed_strategies(state)
        state['strategies']['options'] = {'params': {'version': 'pilot'},
            'stage': 'unvalidated', 'risk_ledger': {'orders': {}}}
        result = {'recommend_promote': True, 'params': {'test': 1}, 'reason': 'fixture'}
        with tempfile.TemporaryDirectory() as d, patch.object(runner.swing, 'research', return_value=copy.deepcopy(result)), patch.object(runner.intraday, 'research', return_value=copy.deepcopy(result)):
            directory = Path(d)
            runner.learn(state, {}, {}, directory, datetime.fromisoformat('2026-10-12T09:30:00-04:00'), defer=True)
            incumbent = state['pending_research']['incumbent']
            self.assertEqual(set(incumbent), {'swing', 'intraday'})
            self.assertEqual(set(incumbent['swing']), {'params'})
            state['strategies']['options']['risk_ledger']['orders']['new-entry'] = {'premium_reserved': '240.00'}
            options_after = copy.deepcopy(state['strategies']['options'])
            applied = runner.apply_pending_research(state, directory, datetime.fromisoformat('2026-10-12T16:01:00-04:00'), False)
        self.assertTrue(applied['swing']['promotion_applied'])
        self.assertTrue(applied['intraday']['promotion_applied'])
        self.assertEqual(state['strategies']['options'], options_after)

    def test_legacy_full_incumbent_accepts_options_added_or_accounting_changed(self):
        for options_existed in (False, True):
            with self.subTest(options_existed=options_existed):
                state = new_state()
                runner.seed_strategies(state)
                if options_existed:
                    state['strategies']['options'] = {'risk_ledger': {'orders': {}}}
                state['pending_research'] = {
                    'evaluated_at': '2026-10-12T09:30:00-04:00',
                    'incumbent': copy.deepcopy(state['strategies']),
                    'results': {'swing': {'recommend_promote': True, 'params': {'test': 2}}}}
                state['strategies']['options'] = {'risk_ledger': {'orders': {'entry': {'premium_reserved': '240.00'}}}}
                state['strategies']['intraday']['stage'] = 'metadata_only_change'
                options_after = copy.deepcopy(state['strategies']['options'])
                with tempfile.TemporaryDirectory() as d:
                    result = runner.apply_pending_research(state, Path(d), datetime.fromisoformat('2026-10-12T16:01:00-04:00'), False)
                self.assertTrue(result['swing']['promotion_applied'])
                self.assertEqual(state['strategies']['options'], options_after)

    def test_real_stock_parameter_change_still_discards_old_or_new_incumbent(self):
        for sleeve in ('swing', 'intraday'):
            for legacy in (False, True):
                with self.subTest(sleeve=sleeve, legacy=legacy):
                    state = new_state()
                    runner.seed_strategies(state)
                    incumbent = copy.deepcopy(state['strategies']) if legacy else {
                        name: {'params': copy.deepcopy(state['strategies'][name]['params'])}
                        for name in ('swing', 'intraday')}
                    state['pending_research'] = {
                        'evaluated_at': '2026-10-12T09:30:00-04:00', 'incumbent': incumbent,
                        'results': {'swing': {'recommend_promote': True, 'params': {'test': 99}}}}
                    state['strategies'][sleeve]['params']['top_n'] = 2
                    before = copy.deepcopy(state['strategies'])
                    with tempfile.TemporaryDirectory() as d:
                        result = runner.apply_pending_research(state, Path(d), datetime.fromisoformat('2026-10-12T16:01:00-04:00'), False)
                    self.assertIsNone(result)
                    self.assertEqual(state['strategies'], before)
                    self.assertNotIn('pending_research', state)
                    self.assertEqual(state['events'][-1]['event'], 'research_candidate_discarded')

    def test_missing_incumbent_stock_parameters_cannot_be_applied(self):
        state = new_state()
        runner.seed_strategies(state)
        before = copy.deepcopy(state['strategies'])
        state['pending_research'] = {
            'evaluated_at': '2026-10-12T09:30:00-04:00',
            'incumbent': {'swing': copy.deepcopy(state['strategies']['swing'])},
            'results': {'swing': {'recommend_promote': True, 'params': {'test': 99}}}}
        with tempfile.TemporaryDirectory() as d:
            result = runner.apply_pending_research(state, Path(d), datetime.fromisoformat('2026-10-12T16:01:00-04:00'), False)
        self.assertIsNone(result)
        self.assertEqual(state['strategies'], before)
        self.assertNotIn('pending_research', state)
