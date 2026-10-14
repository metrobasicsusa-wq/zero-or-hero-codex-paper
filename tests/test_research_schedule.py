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
