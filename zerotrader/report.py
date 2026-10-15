import html
import json
from .storage import ROOT, write_json


def publish(snapshot, research=None):
    directory = ROOT / 'reports'
    directory.mkdir(exist_ok=True)
    write_json(directory / 'latest.json', snapshot)
    if research is not None:
        write_json(directory / 'research-latest.json', research)
    e = lambda x: html.escape(str(x))
    money = lambda x: '${:,.2f}'.format(float(x))
    status = '已触发停机' if snapshot.get('halted') else ('已暂停新开仓' if snapshot.get('paused') else ('自动模拟交易已启用' if snapshot.get('enabled') else '待启用'))
    allocation_note = '；'.join(label + ' ' + money(snapshot['sleeves'][key]['budget'])
                               for key, label in [('swing', '波段正股'), ('intraday', '日内正股'), ('options', '期权试验')]
                               if key in snapshot['sleeves'])
    sleeves = ''
    for key, label in [('swing', '波段选股'), ('intraday', '日内交易')]:
        data = snapshot['sleeves'][key]
        rows = ''.join('<tr><td>'+e(r['symbol'])+'</td><td>'+e(round(r['score'], 3))+'</td><td>'+e(r['reason'])+'</td></tr>' for r in snapshot.get('signals', {}).get(key, []))
        notes = (research or {}).get(key, {})
        stage_note = '候选通过历史验证，已更新参数；未来表现仍待模拟检验。' if notes.get('promotion_applied', notes.get('recommend_promote')) else '当前尚未应用新的候选参数；可能等待收盘或未通过升级门槛。'
        sleeves += f'''<section><h2>{label}</h2><div class="metrics"><div>资金预算<strong>{money(data['budget'])}</strong></div><div>归属净值<strong>{money(data['tracked_equity'])}</strong></div><div>持仓市值<strong>{money(data['market_value'])}</strong></div></div><p>{stage_note}</p><p class="muted">最近选股计算：{e(snapshot.get('selection_at') or '尚未运行')}；交易前会重新计算，不直接执行这张历史候选表。</p><table><thead><tr><th>候选股票</th><th>评分</th><th>选入原因</th></tr></thead><tbody>{rows or '<tr><td colspan="3">当前没有符合条件的候选；允许持有现金。</td></tr>'}</tbody></table><details><summary>当前参数与研究结果</summary><pre>{e(json.dumps({'active_params': data['params'], 'research': notes}, ensure_ascii=False, indent=2))}</pre></details></section>'''
    options = snapshot['sleeves'].get('options')
    if options is not None:
        option_positions = [p for p in snapshot['positions'] if p.get('asset_class') == 'us_option']
        contract_quantity = sum(float(p['qty']) for p in option_positions)
        reservations = options.get('risk_ledger', {}).get('orders', {})
        reserved_premium = sum(float(item.get('premium_reserved', 0)) for item in reservations.values())
        reserved_contracts = sum(int(item.get('contracts_reserved', 0)) for item in reservations.values())
        sleeves += f'''<section><h2>期权模拟试验</h2><div class="metrics"><div>独立预算<strong>{money(options['budget'])}</strong></div><div>归属净值（未计费用）<strong>{money(options['tracked_equity'])}</strong></div><div>当前合约数量<strong>{e(format(contract_quantity, 'g'))} 张</strong></div><div>累计预留权利金<strong>{money(reserved_premium)}</strong></div></div><p>初始最多 4 张合约；累计已预留 {e(reserved_contracts)} 张，每张权利金最多 $240，另留 $10 费用。同时最多 2 张持有或待成交合约；卖出回款不复用。</p><p class="muted">只买标准 100 股乘数的看涨期权，开仓到期区间为 7–21 天。免费 indicative 行情是派生参考报价，并非可执行的 OPRA 报价。此试验尚未验证有效性，权利金可能全部损失；预留数量不是独立实际成交统计。</p></section>'''
    alerts = snapshot.get('alerts', []) + ([snapshot['halted']] if snapshot.get('halted') else [])
    warnings = ''.join('<p class="warning">'+e(item)+'</p>' for item in alerts)
    positions = ''.join(f"<tr><td>{e(p['symbol'])}</td><td>{e(p['qty'])} {'张合约' if p.get('asset_class') == 'us_option' else '股'}</td><td>{money(p['market_value'])}</td><td>{money(p.get('unrealized_pl', 0))}</td></tr>" for p in snapshot['positions'])
    events = ''.join('<li>'+e(json.dumps(item, ensure_ascii=False))+'</li>' for item in reversed(snapshot.get('events', [])))
    markup = f'''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>ZERO OR HERO · 模拟交易控制台</title><style>
    :root{{color-scheme:dark}}body{{margin:0;background:#0b1420;color:#e6edf5;font:15px/1.65 system-ui,"Microsoft YaHei",sans-serif}}main{{max-width:1160px;margin:auto;padding:40px 24px}}header{{border-bottom:1px solid #294052;padding-bottom:25px}}h1{{font-size:36px;margin:10px 0}}h2{{font-size:22px}}.tag{{color:#53e0bd;letter-spacing:2px}}.muted{{color:#9aafc2}}section{{background:#132232;border:1px solid #294052;border-radius:16px;margin-top:22px;padding:26px}}.metrics{{display:flex;flex-wrap:wrap;gap:40px}}.metrics div{{min-width:160px;color:#9aafc2}}strong{{display:block;color:white;font-size:26px}}table{{width:100%;border-collapse:collapse}}td,th{{text-align:left;padding:12px 8px;border-bottom:1px solid #294052}}th{{color:#9aafc2}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px;max-height:500px;overflow:auto}}details{{margin-top:16px}}.warning{{background:#623127;padding:14px;border-radius:9px}}li{{overflow-wrap:anywhere;margin-bottom:10px;font-size:12px}}a{{color:#53e0bd}}footer{{margin:25px 0;color:#9aafc2}}</style><main><header><div class="tag">ZERO OR HERO / PAPER ONLY</div><h1>选股 · 交易 · 策略评估</h1><p>{status} · {'常规交易时段内' if snapshot['market_open'] else '常规交易时段已关闭'}</p><p class="muted">快照时间：{e(snapshot['at'])} · 下次开盘：{e(snapshot['next_open'])}</p></header>{warnings}<section><div class="metrics"><div>模拟账户净值<strong>{money(snapshot['equity'])}</strong></div><div>现金<strong>{money(snapshot['cash'])}</strong></div><div>距净值峰值回撤<strong>{snapshot['drawdown']:.2%}</strong></div><div>停机阈值<strong>{snapshot['drawdown_limit']:.0%}</strong></div></div><p class="muted">总本金上限 $100,000；{allocation_note}；只做多、不使用保证金杠杆，现金预留按配置执行。</p></section>{sleeves}<section><h2>当前持仓</h2><table><thead><tr><th>股票／期权合约</th><th>数量与单位</th><th>市值</th><th>浮动盈亏</th></tr></thead><tbody>{positions or '<tr><td colspan="4">当前空仓</td></tr>'}</tbody></table></section><section><h2>最近执行事件</h2><ul>{events or '<li>尚未提交策略订单。</li>'}</ul></section><footer>这里是静态快照，每次运行更新，重新打开查看最新结果。系统按固定范围比较策略参数，测试不通过则保留基准策略；基准策略也只是模拟实验。历史固定股票池存在幸存者偏差，IEX 行情并非全市场；回测成交与实际模拟成交可能不同。止损与 10% 回撤是触发规则，不能保证损失上限。<p>暂停新开仓：在项目目录执行 <code>.\\Run-Trader.ps1 pause</code>。自动检查仍会管理已有日内仓位和停机退出。</p></footer></main></html>'''
    (directory / 'dashboard.html').write_text(markup, encoding='utf-8')
