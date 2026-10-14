import argparse
from datetime import datetime, timedelta, timezone
import copy
import json
import sys
import os
from . import swing, intraday
from .broker import PaperBroker
from .archive import ResearchArchive
from .archival import bootstrap, capture_fills
from .data import NY, refresh, timestamp
from .engine import Engine, new_state
from .report import publish
from .risk import validate_config
from .samples import build_samples
from .storage import ROOT, data_dir, process_lock, read_json, write_json


def seed_strategies(state):
    for name, module in [('swing', swing), ('intraday', intraday)]:
        state['strategies'].setdefault(name, {'params': dict(module.DEFAULT_PARAMS), 'stage': 'baseline_paper_experiment'})


def weekly_research_due(now, last_research, market_open):
    """Evaluate once on an open Monday from 09:30 NY, starting October 12."""
    local = now.astimezone(NY)
    if not market_open or local.date().isoformat() < '2026-10-12' or local.weekday() != 0 or (local.hour, local.minute) < (9, 30):
        return False
    return not last_research or timestamp(last_research).astimezone(NY).date() < local.date()


def learn(state, daily, minute, directory, now, defer=False):
    results = {}
    incumbent = copy.deepcopy(state['strategies'])
    for name, module, bars in [('swing', swing, daily), ('intraday', intraday, minute)]:
        result = module.research(bars, incumbent=state['strategies'][name]['params'])
        results[name] = result
        result['promotion_applied'] = bool(result['recommend_promote'] and not defer)
        if result['promotion_applied']:
            previous = state['strategies'][name]['params']
            state['strategies'][name] = {'params': result['params'], 'stage': 'historical_validation_passed', 'promoted_at': now.isoformat()}
            state['events'].append({'at': now.isoformat(), 'event': 'strategy_promotion', 'sleeve': name, 'old_params': previous, 'new_params': result['params']})
    if defer:
        state['pending_research'] = {'evaluated_at': now.isoformat(), 'incumbent': incumbent, 'results': copy.deepcopy(results)}
    state['last_research'] = now.isoformat()
    write_json(directory / 'research.json', results)
    write_json(directory / 'research-history' / (now.strftime('%Y%m%d-%H%M%S') + '.json'), results)
    write_json(directory / 'state.json', state)
    return results


def apply_pending_research(state, directory, now, market_open):
    pending = state.get('pending_research')
    if not pending or market_open or state.get('paused') or state.get('halted'):
        return None
    local = now.astimezone(NY)
    evaluated = timestamp(pending['evaluated_at']).astimezone(NY)
    if local.date() == evaluated.date() and local.hour < 16:
        return None
    # A changed incumbent or week-old proposal must be reevaluated, not applied.
    if now - evaluated >= timedelta(days=7) or state['strategies'] != pending['incumbent']:
        state['events'].append({'event': 'research_candidate_discarded', 'at': now.isoformat(), 'reason': 'stale_or_incumbent_changed'})
        state.pop('pending_research')
        write_json(directory / 'state.json', state)
        return None
    results = pending['results']
    for name, result in results.items():
        if result['recommend_promote']:
            previous = state['strategies'][name]['params']
            state['strategies'][name] = {'params': result['params'], 'stage': 'historical_validation_passed', 'promoted_at': now.isoformat()}
            state['events'].append({'at': now.isoformat(), 'event': 'strategy_promotion', 'sleeve': name, 'old_params': previous, 'new_params': result['params']})
            result['promotion_applied'] = True
    state.pop('pending_research')
    write_json(directory / 'research.json', results)
    write_json(directory / 'research-history' / (now.strftime('%Y%m%d-%H%M%S') + '-applied.json'), results)
    write_json(directory / 'state.json', state)
    return results


def make_signals(broker, state, daily, minute, now):
    sessions = broker.calendar((now.date() - timedelta(days=14)).isoformat(), now.date().isoformat())
    previous = [r['date'] for r in sessions if r['date'] < now.date().isoformat()]
    if not previous:
        raise RuntimeError('Cannot establish previous trading session; no selection is usable.')
    expected = max(previous)
    spy = daily.get('SPY', [])
    if not spy or timestamp(spy[-1]['t']).astimezone(NY).date().isoformat() != expected:
        raise RuntimeError('Daily benchmark is stale; new entries blocked.')
    valid_daily = {s: rows for s, rows in daily.items() if rows and timestamp(rows[-1]['t']).astimezone(NY).date().isoformat() == expected}
    results = {
        'swing': swing.rank(valid_daily, state['strategies']['swing']['params'], asof=now.isoformat()),
        'intraday': intraday.rank(minute, state['strategies']['intraday']['params'], asof=now.isoformat())
    }
    # Missing data for a held stock is not a sell signal: omit swing rebalance this cycle.
    if any(o['sleeve'] == 'swing' and s not in valid_daily for s, o in state['owners'].items()):
        results.pop('swing')
    return results


def main():
    parser = argparse.ArgumentParser(description='Alpaca paper-only adaptive stock trader')
    parser.add_argument('action', choices=['status', 'research', 'preview', 'cycle', 'pause', 'export'])
    args = parser.parse_args()
    directory = data_dir()
    archive, run_id, snap, finished = None, None, None, False
    try:
        with process_lock(directory):
            if args.action == 'export':
                archive = ResearchArchive(directory)
                destination = directory / 'exports' / datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
                manifest = archive.export_csv(destination)
                manifest['archive'] = archive.stats()
                manifest['note'] = 'FILL events are execution versions, not cumulative order observations. Group by activity_id before analyzing corrections. Raw inputs remain in the SQLite archive.'
                write_json(destination / 'manifest.json', manifest)
                print(json.dumps(manifest, ensure_ascii=False, indent=2))
                return
            config = read_json(ROOT / 'settings.json')
            validate_config(config)
            if os.environ.get('ZOH_PRIVATE_RUNTIME'):
                migrated = read_json(directory / 'state.json', {})
                if not migrated.get('account_id') or not (directory / 'archive' / 'research.sqlite3').is_file():
                    raise RuntimeError('Cloud execution requires migrated bound state and full archive; fresh bootstrap is blocked.')
            state = read_json(directory / 'state.json', new_state())
            seed_strategies(state)
            if args.action == 'pause':
                state['paused'] = True
                write_json(directory / 'state.json', state)
                print(json.dumps({'paused': True, 'meaning': 'New entries paused; scheduled risk checks and intraday exits remain active.'}))
                return
            archive_error = None
            try:
                archive = ResearchArchive(directory)
                run_id = archive.start_run(args.action, config, state['strategies'])
            except Exception:
                archive_error = 'Research archive unavailable; new entries blocked, risk exits remain enabled.'
            broker = PaperBroker()
            engine = Engine(broker, config, state, directory, audit=archive if run_id else None, run_id=run_id)
            if archive_error:
                engine.audit_available = False
                engine.alerts.append(archive_error)
            # Risk and time-based exits run BEFORE fetching/researching price history.
            snap = engine.cycle({}, execute=args.action == 'cycle')
            now = timestamp(snap['at']).astimezone(NY)
            if run_id:
                try:
                    bootstrap(archive, run_id, directory, broker)
                    capture_fills(archive, run_id, broker, directory, state, now)
                except Exception as exc:
                    engine.audit_available = False
                    message = 'Trade evidence collection failed; new entries blocked: ' + str(exc)
                    engine.alerts.append(message)
                    if message not in snap['alerts']:
                        snap['alerts'].append(message)
            research = read_json(directory / 'research.json', {})
            if args.action == 'cycle' and engine.audit_available:
                before = copy.deepcopy(state['strategies'])
                applied = apply_pending_research(state, directory, now, snap['market_open'])
                if applied is not None:
                    research = applied
                    if run_id:
                        archive.event(run_id, 'research_application', {'results': applied, 'strategies_before': before, 'strategies_after': state['strategies']})
            signals = None
            due = weekly_research_due(now, state['last_research'], snap['market_open'])
            should_research = args.action == 'research' or (args.action == 'cycle' and due)
            if args.action in ('research', 'preview') or (args.action == 'cycle' and (snap['market_open'] or should_research)):
                signals = None
                daily, minute = {}, {}
                try:
                    daily, minute = refresh(broker, config, directory, now, full=should_research)
                    if run_id:
                        metadata = {'feed': 'iex', 'adjustment': 'split', 'observed_at': datetime.now(timezone.utc).isoformat(), 'requested_asof': now.isoformat(), 'cache_meta': read_json(directory / 'data_meta.json', {})}
                        archive.dataset(run_id, 'daily', daily, metadata={**metadata, 'timeframe': '1Day'})
                        archive.dataset(run_id, 'intraday', minute, metadata={**metadata, 'timeframe': '15Min'})
                    if should_research:
                        prior_strategies = copy.deepcopy(state['strategies'])
                        research = learn(state, daily, minute, directory, now, defer=args.action == 'cycle')
                        if run_id:
                            archive.event(run_id, 'research_result', {'results': research, 'strategies_before': prior_strategies, 'strategies_after': state['strategies']})
                    now = timestamp(broker.clock()['timestamp']).astimezone(NY)
                    signals = make_signals(broker, state, daily, minute, now)
                    if run_id:
                        archive.samples(run_id, build_samples(daily, minute, config, state, signals, now))
                        archive.event(run_id, 'selection', {'asof': now.isoformat(), 'signals': signals, 'strategies': state['strategies']})
                except Exception as exc:
                    signals = None
                    snap['alerts'].append('Selection/research could not complete; no further entries this cycle: ' + str(exc))
                    engine.audit_event('selection_failed', {'asof': now.isoformat(), 'error': str(exc)})
                if signals is not None:
                    try:
                        snap = engine.cycle(signals, execute=args.action == 'cycle')
                    except Exception as exc:
                        snap['alerts'].append('Execution stopped; reconcile before further entries: ' + str(exc))
                        publish(snap, research)
                        write_json(directory / 'latest.json', snap)
                        raise
            snap['last_research'] = state['last_research']
            if signals is not None:
                write_json(directory / 'selection.json', {'at': now.isoformat(), 'signals': signals})
            selection = read_json(directory / 'selection.json', {})
            if not snap.get('signals') and selection:
                snap['signals'] = selection['signals']
            snap['selection_at'] = selection.get('at')
            if run_id:
                archive.event(run_id, 'equity_observation', {k: snap[k] for k in ('at', 'equity', 'cash', 'drawdown', 'sleeves')})
                archive.finish_run(run_id, 'completed_with_alerts' if snap['alerts'] else 'completed', snapshot=snap)
                finished = True
                snap['archive'] = archive.stats()
            publish(snap, research)
            write_json(directory / 'latest.json', snap)
            journal = read_json(directory / 'equity-history.json', [])
            journal.append({'at': snap['at'], 'equity': snap['equity'], 'cash': snap['cash'], 'drawdown': snap['drawdown'], 'sleeves': snap['sleeves']})
            write_json(directory / 'equity-history.json', journal[-10000:])
            summary = {k: snap[k] for k in ('at', 'mode', 'enabled', 'paused', 'halted', 'equity', 'cash', 'drawdown', 'market_open', 'last_research', 'actions', 'alerts')}
            summary['signals'] = {name: [r['symbol'] for r in rows] for name, rows in snap.get('signals', {}).items()}
            summary['research'] = {name: {'promoted': r.get('promotion_applied', r['recommend_promote']), 'reason': r['reason']} for name, r in research.items()}
            summary['archive'] = snap.get('archive')
            print(json.dumps(summary, ensure_ascii=False, indent=2))
    except Exception as exc:
        if archive is not None and run_id is not None and not finished:
            try:
                archive.finish_run(run_id, 'failed', snapshot=snap, error=str(exc))
            except Exception:
                pass  # An unavailable disk cannot prevent risk work already completed.
        # All network/credential errors are sanitized by the broker boundary.
        print(json.dumps({'error': str(exc), 'mode': 'PAPER', 'new_entries': 'blocked'}, ensure_ascii=False))
        sys.exit(1)
