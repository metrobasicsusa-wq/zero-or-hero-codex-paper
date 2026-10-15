"""Import available legacy evidence and capture precise broker executions."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from .storage import read_json, write_json
from .data import timestamp


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def bootstrap(archive, run_id, directory, broker):
    marker = directory / 'archive-backfill.json'
    if marker.exists():
        return
    imported = datetime.now(timezone.utc).isoformat()
    for name in ('state.json', 'selection.json', 'research.json', 'paper-probe.json', 'data_meta.json'):
        payload = read_json(directory / name)
        if payload is not None:
            evidence = {'source_file': name, 'original_inputs_not_reconstructed': True, 'data': payload}
            # The marker is written only after the whole bootstrap succeeds. A
            # retry may see a newer state/cache, and must preserve both versions
            # without reusing an immutable key for different evidence.
            archive.event_once(run_id, 'legacy_import', evidence,
                               key='legacy-file:' + name + ':' + fingerprint(evidence))
    for row in read_json(directory / 'equity-history.json', []):
        archive.event_once(run_id, 'legacy_equity', row, key='legacy-equity:' + fingerprint(row))
    for path in sorted((directory / 'research-history').glob('*.json')):
        evidence = {'source_file': path.name, 'original_code_version_unknown': True, 'data': read_json(path)}
        archive.event_once(run_id, 'legacy_research', evidence,
                           key='legacy-research:' + path.name + ':' + fingerprint(evidence))
    for name in ('daily', 'intraday'):
        rows = read_json(directory / (name + '.json'))
        if rows:
            archive.dataset(run_id, 'imported_' + name, rows, metadata={'source': 'existing_cache', 'observed_at': imported, 'historical_availability_not_proven': True, 'feed': 'iex', 'adjustment': 'split'})
    probe = read_json(directory / 'paper-probe.json', {})
    if probe.get('order_id'):
        order = broker.get_order(probe['order_id'])
        evidence = {'source': 'broker_current_receipt', 'is_strategy_trade': False, 'order': order}
        archive.event_once(run_id, 'integration_probe_order', evidence,
                           key='probe:' + probe['order_id'] + ':' + fingerprint(evidence))
    write_json(marker, {'at': imported, 'run_id': run_id, 'note': 'Only available evidence imported. No missing historical samples invented.'})


def capture_fills(archive, run_id, broker, directory, state, now):
    """Collect actual FILL versions plus separate option lifecycle evidence.

    Callers must persist any state change through Engine.save in a finally block
    (CloudCoordinator._state_operation in the cloud). In particular a lifecycle
    review pause must survive a later archive/cursor failure. This collector never
    submits, cancels, exercises, adopts holdings or writes state.json itself.
    """
    cursor_path = directory / 'fill-archive-cursor.json'
    cursor = read_json(cursor_path, {})
    after = (timestamp(cursor['last_successful_poll']) - timedelta(days=1)).isoformat() if cursor.get('last_successful_poll') else None
    fills = broker.fill_activities(after=after, until=now.isoformat())
    if not isinstance(fills, list):
        raise ValueError('Fill activity response must be a complete list.')
    for fill in fills:
        if not isinstance(fill, dict) or not isinstance(fill.get('id'), str) or not fill['id'] or not isinstance(fill.get('order_id'), str) or not fill['order_id']:
            raise ValueError('Fill activity requires stable activity and order IDs.')
        order_id = fill['order_id']
        sleeve = state.get('fills', {}).get(order_id, {}).get('sleeve')
        if sleeve is None:
            sleeve = next((v.get('sleeve') for v in state.get('intents', {}).values() if v.get('order_id') == order_id), None)
        # Broker evidence has an immutable identity independent of local sleeve
        # reconciliation. Corrections are new versions of the same activity ID;
        # they are not additional economic fills and must not be summed together.
        version = fingerprint(fill)
        archive.event_once(run_id, 'broker_fill_activity',
                           {'source': 'alpaca_account_activities_FILL', 'activity_id': fill['id'],
                            'activity_hash': version, 'activity': fill},
                           key='broker-fill-raw-v1:' + fill['id'] + ':' + version)
        attribution = {'source': 'local_order_attribution', 'activity_id': fill['id'],
                       'activity_hash': version, 'order_id': order_id, 'sleeve': sleeve}
        archive.event_once(run_id, 'broker_fill_attribution', attribution,
                           key='broker-fill-attribution:' + fill['id'] + ':' + fingerprint(attribution))
    if 'options' in state.get('strategies', {}):
        capture_options_lifecycle(archive, run_id, broker, state, now, cursor)
        cursor.update(option_lifecycle_last_successful_poll=now.isoformat(), option_lifecycle_overlap_days=2)
    cursor.update(last_successful_poll=now.isoformat(), overlap_days=1)
    write_json(cursor_path, cursor)
    return len(fills)


def capture_options_lifecycle(archive, run_id, broker, state, now, cursor):
    """Versioned option NTAs are never FILLs or authority to adopt stock.

    The provider's economic date may precede availability. Overlap the prior
    successful poll by two full days, and keep every content correction under
    its original activity ID. A newly observed lifecycle version latches only
    new-entry pause; protective exits remain the engine's responsibility.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError('Option lifecycle polling requires a timezone-aware clock.')
    previous = cursor.get('option_lifecycle_last_successful_poll')
    anchor = timestamp(previous) if previous else now
    if anchor.tzinfo is None or anchor.utcoffset() is None or anchor > now:
        raise ValueError('Invalid option lifecycle polling cursor.')
    activities = broker.options_lifecycle_activities(after=(anchor - timedelta(days=2)).isoformat(), until=now.isoformat())
    if not isinstance(activities, list):
        raise ValueError('Option lifecycle response must be a complete list.')
    allowed = {'OPEXC', 'OPTRD', 'OPASN', 'OPEXP'}
    strategy = state['strategies']['options']
    if not isinstance(strategy, dict) or not isinstance(strategy.get('lifecycle_reviews', {}), dict):
        raise ValueError('Invalid option lifecycle review ledger.')
    observed_versions = set()
    for activity in activities:
        if (not isinstance(activity, dict) or not isinstance(activity.get('id'), str) or not activity['id']
                or activity.get('activity_type') not in allowed or not isinstance(activity.get('date'), str)):
            raise ValueError('Option lifecycle evidence requires an activity ID, type and economic date.')
        economic = timestamp(activity['date'])
        if len(activity['date']) != 10 and (economic.tzinfo is None or economic.utcoffset() is None):
            raise ValueError('Option lifecycle timestamp must include a timezone.')
        version = fingerprint(activity)
        identity = activity['id'] + ':' + version
        if identity in observed_versions:
            continue
        observed_versions.add(identity)
        archive.event_once(run_id, 'broker_option_lifecycle_activity', {
            'source': 'alpaca_option_non_trade_activities', 'activity_id': activity['id'],
            'activity_hash': version, 'activity': activity, 'is_fill': False,
            'economic_date_is_not_observation_time': True}, key='broker-option-lifecycle-raw-v1:' + identity)
        symbol = activity.get('symbol')
        entries = [cid for cid, intent in state.get('intents', {}).items()
                   if intent.get('sleeve') == 'options' and intent.get('payload', {}).get('side') == 'buy'
                   and isinstance(symbol, str) and intent['payload'].get('symbol') == symbol
                   and intent.get('contract', {}).get('symbol') == symbol]
        attribution = {'activity_id': activity['id'], 'activity_hash': version,
            'source': 'local_exact_option_contract_lineage', 'entry_client_order_ids': sorted(entries),
            'attributed_to_options_pilot': bool(entries), 'is_fill': False,
            'underlying_stock_adopted': False, 'inventory_change_independently_reconciled': False}
        archive.event_once(run_id, 'broker_option_lifecycle_attribution', attribution,
                           key='broker-option-lifecycle-attribution-v1:' + identity + ':' + fingerprint(attribution))
        notice_key = fingerprint({'activity_id': activity['id'], 'activity_hash': version})
        reviews = strategy.setdefault('lifecycle_reviews', {})
        if notice_key in reviews:
            continue
        # Pause before writing the diagnostic, so a diagnostic write failure is
        # still captured by the caller's finally/save persistence boundary.
        state['paused'] = True
        alert = {'event': 'options_lifecycle_manual_review', 'at': now.isoformat(),
                 'activity_id': activity['id'], 'activity_hash': version,
                 'activity_type': activity['activity_type'], 'symbol': symbol,
                 'is_fill': False, 'manual_review_required': True,
                 'reason': 'Option expiration/exercise/assignment/trade lifecycle requires inventory reconciliation; no stock adopted.'}
        archive.event_once(run_id, 'options_lifecycle_manual_review',
                           {key: value for key, value in alert.items() if key != 'at'},
                           key='option-lifecycle-review-v1:' + notice_key)
        reviews[notice_key] = {'activity_id': activity['id'], 'activity_hash': version,
                              'activity_type': activity['activity_type'], 'detected_at': now.isoformat(),
                              'manual_review_required': True}
        state['events'] = (state.get('events', []) + [alert])[-500:]
    return len(observed_versions)
