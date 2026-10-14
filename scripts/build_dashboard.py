"""Build a credential-free, standalone public PAPER dashboard from local evidence."""
import gzip
import json
import os
import sqlite3
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = Path(os.environ['LOCALAPPDATA']) / 'ZeroOrHero'


def evidence(db, start, cutoff):
    fills, attribution, intents, receipts = {}, {}, {}, {}
    orders, order_at, promotions = [], None, {'swing': 0, 'intraday': 0}
    for at, kind, encoding, raw in db.execute("SELECT e.at,e.kind,b.encoding,b.payload FROM events e JOIN blobs b ON e.payload_hash=b.sha256 WHERE e.kind IN ('broker_fill_activity','broker_fill_attribution','order_intent','order_response','broker_snapshot','strategy_promotion') ORDER BY e.sequence"):
        if datetime.fromisoformat(at.replace('Z', '+00:00')) > cutoff:
            continue
        item = json.loads(gzip.decompress(raw) if 'gzip' in encoding else raw)
        if kind == 'broker_fill_activity':
            fills[item['activity_id']] = item['activity']
        elif kind == 'broker_fill_attribution':
            attribution[item['activity_id']] = item.get('sleeve')
        elif kind == 'order_intent':
            intents[item['client_order_id']] = item
        elif kind == 'order_response':
            receipts[item['broker_order']['id']] = item['client_order_id']
        elif kind == 'broker_snapshot':
            orders, order_at = item['orders'], item['clock']['timestamp']
        elif kind == 'strategy_promotion' and item.get('sleeve') in promotions:
            promotions[item['sleeve']] += 1
    trades = []
    for aid, fill in fills.items():
        when = fill.get('transaction_time')
        if not when or not start <= datetime.fromisoformat(when.replace('Z', '+00:00')) <= cutoff:
            continue
        sleeve = attribution.get(aid)
        if sleeve not in promotions:
            continue  # Unattributed/test activity is not presented as strategy execution.
        intent = intents.get(receipts.get(fill.get('order_id')), {})
        signal = (intent.get('context') or {}).get('signal') or {}
        reason = signal.get('reason') if isinstance(signal, dict) else None
        trades.append({'at': when, 'symbol': fill['symbol'], 'side': fill['side'], 'qty': fill['qty'], 'price': fill['price'], 'sleeve': sleeve, 'reason': reason or ('退出原因参见逐笔审核' if fill['side'] == 'sell' else '未关联到下单前信号')})
    stops, seen = [], set()
    def walk(rows):
        for order in rows:
            if order.get('id') in seen:
                continue
            seen.add(order.get('id'))
            if order.get('side') == 'sell' and order.get('type') in ('stop', 'stop_limit') and order.get('status') in ('new', 'accepted', 'pending_new', 'partially_filled'):
                stops.append({k: order.get(k) for k in ('symbol', 'qty', 'filled_qty', 'stop_price', 'time_in_force', 'status')})
            walk(order.get('legs') or [])
    walk(orders)
    return {'trades': sorted(trades, key=lambda x: x['at'], reverse=True), 'stops': stops, 'orders_at': order_at, 'promotions': promotions}


def build():
    latest = json.loads((ROOT / 'reports/latest.json').read_text(encoding='utf-8-sig'))
    cloud = json.loads((RUNTIME / 'paper-500/dashboard-observation.json').read_text(encoding='utf-8-sig'))
    db = sqlite3.connect((RUNTIME / 'autotrader/archive/research.sqlite3').as_uri() + '?mode=ro', uri=True)
    db.execute('BEGIN')
    observations = []
    start = datetime.fromisoformat('2026-09-30T17:13:00-04:00')
    cutoff = datetime.fromisoformat(latest['at'])
    for at, encoding, payload in db.execute("SELECT e.at,b.encoding,b.payload FROM events e JOIN blobs b ON e.payload_hash=b.sha256 WHERE e.kind='equity_observation' ORDER BY e.sequence"):
        item = json.loads(gzip.decompress(payload) if 'gzip' in encoding else payload)
        observation_at = datetime.fromisoformat(item.get('at', at).replace('Z', '+00:00'))
        if item.get('equity') is not None and start <= observation_at <= cutoff:
            observations.append({'at': item.get('at', at), 'equity': float(item['equity'])})
    extra = evidence(db, start, cutoff)
    db.close()
    monitor = json.loads((RUNTIME / 'autotrader/monitor-state.json').read_text(encoding='utf-8-sig'))
    reviews = [{'date': day, 'status': item.get('audit_status', 'post_market_review')} for day, item in monitor.get('daily_reviews', {}).items() if item.get('status') == 'complete' and re.fullmatch(r'\d{4}-\d{2}-\d{2}', day)]
    # Select observed points per ten-minute bucket, never create interpolated observations.
    points = {}
    for item in observations:
        t = datetime.fromisoformat(item['at'].replace('Z', '+00:00'))
        points[int(t.timestamp()) // 600] = item
    payload = {
        **extra,
        'reviews': sorted(reviews, key=lambda x: x['date'], reverse=True),
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'main': {k: latest.get(k) for k in ('at', 'equity', 'cash', 'drawdown', 'paused', 'halted', 'last_research') if k != 'halted'},
        'halted': bool(latest.get('halted')),
        'positions': [{k: p.get(k) for k in ('symbol', 'qty', 'avg_entry_price', 'current_price', 'market_value', 'unrealized_pl')} for p in latest['positions']],
        'sleeves': {k: {f: v[f] for f in ('budget', 'tracked_equity', 'market_value', 'params')} for k, v in latest['sleeves'].items()},
        'curve': list(points.values()),
        'cloud': {k: cloud.get(k) for k in ('at', 'status', 'cash', 'equity', 'positions_count', 'open_orders_count', 'trading_enabled')},
    }
    markup = (ROOT / 'scripts/dashboard-template.html').read_text(encoding='utf-8')
    data = json.dumps(payload, ensure_ascii=False).replace('<', '\\u003c')
    for pattern in (r'github_pat_', r'ghp_', r'APCA-API', r'account_id', r'order_id', r'client_order_id', r'(?i)\\Users\\', r'[A-Za-z]:\\', r'\b[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}\b'):
        if re.search(pattern, data):
            raise ValueError('Public data validation failed')
    output = ROOT / 'dashboard-public'
    output.mkdir(exist_ok=True)
    (output / 'index.html').write_text(markup.replace('__DATA__', data), encoding='utf-8')
    (output / '.nojekyll').touch()
    print(json.dumps({'points': len(points), 'positions': len(payload['positions']), 'fills': len(extra['trades']), 'stops': len(extra['stops']), 'main_as_of': latest['at'], 'cloud_as_of': cloud['at']}))


if __name__ == '__main__':
    build()
