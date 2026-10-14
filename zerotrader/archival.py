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
    write_json(cursor_path, {'last_successful_poll': now.isoformat(), 'overlap_days': 1})
    return len(fills)
