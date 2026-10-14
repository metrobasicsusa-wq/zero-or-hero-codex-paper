"""Bounded, long-only opening-range research for completed Alpaca 15Min bars.

This module is pure calculation: it has no broker, credential, or order access.
Alpaca ``t`` is the bar's opening time. A signal is available 15 minutes later.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta
import math
from statistics import mean
from zoneinfo import ZoneInfo


NY = ZoneInfo("America/New_York")
BAR = timedelta(minutes=15)
DEFAULT_PARAMS = {"opening_range_minutes": 30, "volume_ratio": 1.5, "top_n": 3,
                  "stop_pct": 0.015}
MIN_SESSIONS = 60
COST_PER_SIDE = 0.001
LIMITATIONS = [
    "Research uses approximate bar fills, not actual executable quotes or queue position.",
    "IEX bars cover one exchange; volume and prices can differ from consolidated US markets.",
    "A completed-bar signal fills at the immediately following 15-minute bar open; "
    "10 bps per side is an assumed cost, not a measured execution cost.",
    "Stops use bar lows and gap-open prices; intrabar order timing is unknown.",
    "Positions close by 15:45 New York, or at the last available regular-session "
    "bar close on a shorter day. An early close cannot be distinguished from missing data.",
    "The supplied symbol universe may contain survivorship and selection bias; "
    "results do not establish future profitability.",
    "Holdout is chronological. Repeated weekly reuse of overlapping holdout dates "
    "is not independent evidence and must be tracked by the caller.",
]


def _params(value: dict | None = None) -> dict:
    source = value or {}
    if isinstance(source.get("params"), dict):
        source = source["params"]
    p = dict(DEFAULT_PARAMS)
    p.update({k: source[k] for k in p if k in source})
    if p["opening_range_minutes"] not in (30, 60):
        raise ValueError("opening_range_minutes must be 30 or 60")
    if p["volume_ratio"] not in (1.0, 1.5):
        raise ValueError("volume_ratio must be 1.0 or 1.5")
    if p["top_n"] not in (2, 3):
        raise ValueError("top_n must be 2 or 3")
    if p["stop_pct"] != DEFAULT_PARAMS["stop_pct"]:
        raise ValueError("intraday stop_pct is fixed at 0.015")
    return p


def _dt(value: str | datetime) -> datetime:
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamps must have a timezone")
    return result.astimezone(NY)


def _prepare(bars: dict, cutoff: datetime | None = None) -> dict:
    result = {}
    for symbol, rows in bars.items():
        unique = {}
        for row in rows:
            try:
                stamp = _dt(row["t"])
                values = {key: float(row[key]) for key in ("o", "h", "l", "c", "v")}
                if not all(math.isfinite(v) for v in values.values()):
                    continue
                if not (0 < values["l"] <= min(values["o"], values["c"])
                        <= max(values["o"], values["c"]) <= values["h"] and values["v"] >= 0):
                    continue
                if stamp.second or stamp.microsecond or stamp.minute % 15:
                    continue
                if not time(9, 30) <= stamp.time() < time(16):
                    continue
                if cutoff is not None and stamp + BAR > cutoff:
                    continue
                unique[stamp] = dict(values, t=stamp)
            except (KeyError, TypeError, ValueError, AttributeError):
                continue
        result[str(symbol).upper()] = sorted(unique.values(), key=lambda r: r["t"])
    return result


def _candidate(symbol: str, today: list[dict], previous: list[dict], p: dict,
               slot_volumes: dict | None = None) -> dict | None:
    if not today:
        return None
    last = today[-1]
    signal_time = last["t"] + BAR
    if not time(10) <= signal_time.time() <= time(14, 30):
        return None
    opening = last["t"].replace(hour=9, minute=30, second=0, microsecond=0)
    range_end = opening + timedelta(minutes=p["opening_range_minutes"])
    if last["t"] < range_end:
        return None
    range_bars = [r for r in today if opening <= r["t"] < range_end]
    expected = {opening + i * BAR for i in range(p["opening_range_minutes"] // 15)}
    if {r["t"] for r in range_bars} != expected:
        return None
    range_high = max(r["h"] for r in range_bars)
    total_volume = sum(r["v"] for r in today)
    if total_volume <= 0:
        return None
    vwap = sum(((r["h"] + r["l"] + r["c"]) / 3) * r["v"] for r in today) / total_volume
    same_slot = (slot_volumes.get(last["t"].time(), [])[-20:] if slot_volumes is not None
                 else [r["v"] for r in previous if r["t"].time() == last["t"].time()][-20:])
    baseline = same_slot if len(same_slot) >= 5 else [r["v"] for r in today[:-1]]
    average_volume = mean(baseline) if baseline else 0
    ratio = last["v"] / average_volume if average_volume > 0 else 0
    if last["c"] <= range_high or last["c"] <= vwap or ratio < p["volume_ratio"]:
        return None
    score = 100 * (last["c"] / range_high - 1) + 100 * (last["c"] / vwap - 1) + min(ratio, 5) - 1
    return {"symbol": symbol, "score": round(score, 8), "price": last["c"],
            "reason": f"{p['opening_range_minutes']}m opening-range breakout; "
                      f"volume {ratio:.2f}x; close above session VWAP {vwap:.4f}",
            "signal_time": signal_time.isoformat(), "vwap": round(vwap, 8),
            "volume_ratio": round(ratio, 8), "stop_pct": p["stop_pct"]}


def rank(bars: dict[str, list], params: dict, asof: str | None = None) -> list[dict]:
    """Rank the latest completed bar of the current NY session, with no future rows.

    Callers must also check the exchange clock, quote freshness, portfolio limits,
    and their persistent per-symbol/per-day entry ledger before executing signals.
    """
    p = _params(params)
    now = _dt(asof) if asof else None
    prepared = _prepare(bars, now)
    if now is None:
        endings = [rows[-1]["t"] + BAR for rows in prepared.values() if rows]
        if not endings:
            return []
        now = max(endings)
    if not time(10) <= now.time() <= time(14, 30):
        return []
    candidates = []
    for symbol, rows in prepared.items():
        today = [r for r in rows if r["t"].date() == now.date()]
        # A missing current bar is not a fresh actionable signal.
        if not today or now - (today[-1]["t"] + BAR) >= BAR:
            continue
        earlier = [r for r in rows if r["t"].date() < now.date()]
        candidate = _candidate(symbol, today, earlier, p)
        if candidate:
            candidates.append(candidate)
    return sorted(candidates, key=lambda r: (-r["score"], r["symbol"]))[:p["top_n"]]


def _simulate(prepared: dict, p: dict) -> list[dict]:
    """Create deterministic day trades. Daily entries never exceed top_n.

    Each entry receives 1/top_n of that day's sleeve equity; cash remains idle
    where fewer signals occur. Entries on a given bar are ranked together.
    """
    grouped = {symbol: defaultdict(list) for symbol in prepared}
    all_dates = set()
    for symbol, rows in prepared.items():
        for row in rows:
            grouped[symbol][row["t"].date()].append(row)
            all_dates.add(row["t"].date())
    previous = {symbol: [] for symbol in prepared}
    trades = []
    for day in sorted(all_dates):
        opportunities = defaultdict(list)
        for symbol in prepared:
            rows = grouped[symbol].get(day, [])
            slot_volumes = defaultdict(list)
            for historical in previous[symbol]:
                slot_volumes[historical["t"].time()].append(historical["v"])
            for index, row in enumerate(rows[:-1]):
                candidate = _candidate(symbol, rows[:index + 1], previous[symbol], p, slot_volumes)
                if candidate is None:
                    continue
                following = rows[index + 1]
                # Gaps cannot masquerade as an executable next-bar fill.
                if following["t"] != row["t"] + BAR:
                    continue
                opportunities[following["t"]].append((candidate, rows[index + 1:]))
        entered = set()
        for entry_time in sorted(opportunities):
            choices = sorted(opportunities[entry_time], key=lambda v: (-v[0]["score"], v[0]["symbol"]))
            for candidate, later in choices:
                symbol = candidate["symbol"]
                if symbol in entered or len(entered) >= p["top_n"]:
                    continue
                entry = later[0]["o"]
                stop = entry * (1 - p["stop_pct"])
                # Entire exit bars must close by 15:45 (last start 15:30).
                exit_rows = [r for r in later if (r["t"] + BAR).time() <= time(15, 45)]
                if not exit_rows:
                    continue
                exit_price, exit_time, exit_reason = exit_rows[-1]["c"], exit_rows[-1]["t"] + BAR, "session_close"
                for row in exit_rows:
                    if row["o"] <= stop:
                        exit_price, exit_time, exit_reason = row["o"], row["t"], "stop_gap"
                        break
                    if row["l"] <= stop:
                        exit_price, exit_time, exit_reason = stop, row["t"] + BAR, "stop"
                        break
                net = exit_price * (1 - COST_PER_SIDE) / (entry * (1 + COST_PER_SIDE)) - 1
                trades.append({"symbol": symbol, "date": day.isoformat(),
                               "entry_time": entry_time.isoformat(), "exit_time": exit_time.isoformat(),
                               "entry_price": entry, "exit_price": exit_price, "exit_reason": exit_reason,
                               "net_return": net, "weight": 1 / p["top_n"]})
                entered.add(symbol)
        for symbol in prepared:
            previous[symbol].extend(grouped[symbol].get(day, []))
            # History is only needed for the last 20 same-slot observations.
            if len(previous[symbol]) > 26 * 30:
                previous[symbol] = previous[symbol][-26 * 30:]
    return trades


def _metrics(trades: list[dict], days: list[date]) -> dict:
    dates = {d.isoformat() for d in days}
    selected = [trade for trade in trades if trade["date"] in dates]
    daily = defaultdict(float)
    for trade in selected:
        daily[trade["date"]] += trade["weight"] * trade["net_return"]
    equity = peak = 1.0
    max_dd = 0.0
    for day in days:
        equity *= 1 + daily[day.isoformat()]
        peak = max(peak, equity)
        max_dd = max(max_dd, 1 - equity / peak)
    returns = [t["net_return"] for t in selected]
    return {"sessions": len(days), "trades": len(selected),
            "net_return": round(equity - 1, 8),
            "expectancy": round(mean(returns), 8) if returns else 0.0,
            "max_drawdown": round(max_dd, 8),
            "win_rate": round(sum(r > 0 for r in returns) / len(returns), 8) if returns else 0.0,
            "cost_bps_per_side": 10, "drawdown_basis": "closed-trade daily sleeve equity"}


def _name(p: dict) -> str:
    return f"orb{p['opening_range_minutes']}-volume{p['volume_ratio']:g}-top{p['top_n']}"


def _passes(metrics: dict, min_trades: int) -> bool:
    return (metrics["trades"] >= min_trades and metrics["expectancy"] > 0
            and metrics["net_return"] > 0 and metrics["max_drawdown"] <= 0.08)


def research(bars: dict[str, list], incumbent: dict | None = None) -> dict:
    """Chronological bounded search; validation selects, holdout only confirms.

    No parameters can change the fixed stop or portfolio-level risk policy.
    ``recommend_promote`` is advisory and must gate any caller-side activation.
    """
    prepared = _prepare(bars)
    days = sorted({row["t"].date() for rows in prepared.values() for row in rows})
    baseline = _params(incumbent)
    if len(days) < MIN_SESSIONS:
        return {"params": baseline, "recommend_promote": False, "selected_name": _name(baseline),
                "metrics": {}, "validation": {"passed": False, "sessions": len(days),
                "minimum_sessions": MIN_SESSIONS, "minimum_holdout_trades": 20},
                "reason": f"Need at least {MIN_SESSIONS} sessions; received {len(days)}.",
                "rankings": [], "limitations": list(LIMITATIONS)}
    train_end = int(len(days) * 0.6)
    validation_end = int(len(days) * 0.8)
    splits = {"train": days[:train_end], "validation": days[train_end:validation_end],
              "holdout": days[validation_end:]}
    records = []
    for opening in (30, 60):
        for volume in (1.0, 1.5):
            for top_n in (2, 3):
                p = _params({"opening_range_minutes": opening, "volume_ratio": volume, "top_n": top_n})
                trades = _simulate(prepared, p)
                train = _metrics(trades, splits["train"])
                validation = _metrics(trades, splits["validation"])
                records.append({"name": _name(p), "params": p, "train": train, "validation": validation,
                                "eligible": _passes(train, 20) and _passes(validation, 10), "trades": trades})
    # Holdout metrics do not participate in candidate selection or eligibility.
    records.sort(key=lambda r: (r["eligible"], r["validation"]["net_return"]
                               - r["validation"]["max_drawdown"], r["train"]["net_return"],
                               -r["params"]["top_n"], -r["params"]["opening_range_minutes"]), reverse=True)
    chosen = records[0]
    incumbent_record = next(r for r in records if r["params"] == baseline)
    if not chosen["eligible"]:
        chosen = incumbent_record
    holdout = _metrics(chosen["trades"], splits["holdout"])
    incumbent_holdout = _metrics(incumbent_record["trades"], splits["holdout"])
    promote = chosen["eligible"] and _passes(holdout, 20)
    reason = "Selected on training/validation; independent chronological holdout passed minimum gates."
    if not chosen["eligible"]:
        reason = "No candidate passed training and validation gates; preserve incumbent parameters."
    elif not _passes(holdout, 20):
        reason = "Holdout did not meet 20 trades, positive net expectancy/return and <=8% drawdown gates."
    elif incumbent is not None and chosen["params"] == baseline:
        promote = False
        reason = "Incumbent already has the selected parameters; no parameter change is recommended."
    elif incumbent is not None and holdout["net_return"] <= incumbent_holdout["net_return"]:
        promote = False
        reason = "Selected candidate did not improve holdout net return over incumbent; preserve incumbent."
    return {"params": chosen["params"], "recommend_promote": bool(promote), "selected_name": chosen["name"],
            "metrics": {"train": chosen["train"], "validation": chosen["validation"], "holdout": holdout},
            "validation": {"passed": bool(promote), "sessions": len(days), "minimum_sessions": MIN_SESSIONS,
                           "minimum_holdout_trades": 20, "max_drawdown_gate": 0.08,
                           "split_dates": {key: {"start": values[0].isoformat(), "end": values[-1].isoformat(),
                                                 "sessions": len(values)} for key, values in splits.items()},
                           "incumbent_holdout": incumbent_holdout,
                           "selection_uses_holdout": False},
            "reason": reason,
            "rankings": [{k: v for k, v in record.items() if k != "trades"} for record in records],
            "limitations": list(LIMITATIONS)}
