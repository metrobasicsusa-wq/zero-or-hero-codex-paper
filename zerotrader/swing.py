"""Bounded, deterministic swing research for the Alpaca paper account.

Only strategy parameters are selected. Capital, the 5% protective stop, transaction
costs and risk gates cannot be optimized. Daily inputs must be completed, adjusted
bars; SPY is a market filter and benchmark, never a traded symbol.
"""

from __future__ import annotations

import itertools
import math
import statistics
from datetime import date
from typing import Any


DEFAULT_PARAMS = {"momentum": 60, "trend": 60, "top_n": 5, "stop_pct": 0.05}
_ALLOWED = {"momentum": (20, 60, 120), "trend": (60, 120), "top_n": (3, 5)}
_CAPITAL = 50_000.0
_COST = 0.001  # 10 basis points for each buy and each sell.
_MAX_DRAWDOWN = 0.08  # Research gate leaves room before the account's 10% halt.
_IMPROVEMENT = 0.0025
_WARMUP = 121  # First executable day after 120 previous daily returns.
_MIN_SESSIONS = 400

LIMITATIONS = [
    "Research only: repeated weekly use of rolling holdouts is not fresh independent evidence.",
    "A fixed current symbol universe creates survivorship and selection bias.",
    "Signals use completed daily bars; next-open fills plus 10 bps per side approximate live 09:45 limit orders.",
    "Retained shares are not resized; new slots are capped at $50,000/top_n and a 2% sleeve cash reserve approximates the account-wide reserve.",
    "Independent sleeve research cannot reproduce the other sleeve's cash usage, overlapping symbols or pending live orders.",
    "Daily OHLC cannot establish intraday event order or stop liquidity; gap stops fill at the open.",
    "Drawdown is measured on daily closing equity and may understate intraday drawdown.",
    "IEX-only volumes/prices differ from consolidated market data; corporate-action adjustments are required.",
    "A 5% stop does not cap realized loss across gaps; cash earns no interest and dividends are not paid separately.",
]


def _params(value: dict | None) -> dict:
    result = dict(DEFAULT_PARAMS)
    if value is not None:
        unknown = set(value) - set(result)
        if unknown:
            raise ValueError("Unsupported strategy parameter(s): " + ", ".join(sorted(unknown)))
        result.update(value)
    for name, values in _ALLOWED.items():
        if isinstance(result[name], bool) or result[name] not in values:
            raise ValueError(f"{name} must be one of {values}")
        result[name] = int(result[name])
    if isinstance(result["stop_pct"], bool) or result["stop_pct"] != 0.05:
        raise ValueError("stop_pct is fixed at 0.05 and cannot evolve")
    return result


def _name(params: dict) -> str:
    return f"momentum{params['momentum']}_trend{params['trend']}_top{params['top_n']}"


def _day(value: Any) -> str:
    result = str(value)[:10]
    date.fromisoformat(result)
    return result


class _Series:
    def __init__(self, rows: list[dict], cutoff: str | None):
        by_date: dict[str, dict] = {}
        for raw in rows:
            try:
                day = _day(raw["t"])
                if cutoff and day > cutoff:
                    continue
                row = {field: float(raw[field]) for field in ("o", "h", "l", "c")}
                volume = float(raw.get("v", 0))
                if not all(math.isfinite(x) and x > 0 for x in row.values()):
                    continue
                if not math.isfinite(volume) or volume < 0:
                    continue
                if row["l"] > min(row["o"], row["c"]) or row["h"] < max(row["o"], row["c"]):
                    continue
                row.update(t=day, v=volume)
                by_date[day] = row
            except (KeyError, ValueError, TypeError, OverflowError):
                continue
        self.days = sorted(by_date)
        self.rows = [by_date[day] for day in self.days]
        self.index = {day: i for i, day in enumerate(self.days)}
        self.closes = [row["c"] for row in self.rows]
        self.sums = [0.0]
        for close in self.closes:
            self.sums.append(self.sums[-1] + close)
        self.returns = [0.0] + [self.closes[i] / self.closes[i - 1] - 1 for i in range(1, len(self.closes))]

    def average(self, index: int, period: int) -> float:
        return (self.sums[index + 1] - self.sums[index + 1 - period]) / period


class _Market:
    def __init__(self, bars: dict[str, list[dict]], asof: str | None = None):
        cutoff = _day(asof) if asof is not None else None
        self.series = {symbol.upper(): _Series(rows, cutoff) for symbol, rows in bars.items()}
        self.spy = self.series.get("SPY")
        self.days = self.spy.days if self.spy else []
        self.signals: dict[tuple, list[dict]] = {}

    def row(self, symbol: str, day: str) -> dict | None:
        series = self.series[symbol]
        index = series.index.get(day)
        return series.rows[index] if index is not None else None


def _rank(market: _Market, params: dict, index: int) -> list[dict]:
    key = (params["momentum"], params["trend"], params["top_n"], index)
    if key in market.signals:
        return market.signals[key]
    if index < 119 or index >= len(market.days) or not market.spy:
        return []
    spy = market.spy
    if spy.closes[index] <= spy.average(index, 120):
        market.signals[key] = []
        return []
    day = market.days[index]
    period, trend = params["momentum"], params["trend"]
    output = []
    for symbol, series in market.series.items():
        if symbol == "SPY":
            continue
        i = series.index.get(day)
        if i is None or i < max(period, trend - 1, 20):
            continue
        # Exclude stale/gappy inputs: the last 21 prices must match SPY sessions.
        if series.days[i - 20 : i + 1] != market.days[index - 20 : index + 1]:
            continue
        price = series.closes[i]
        if price < 10 or price <= series.average(i, trend):
            continue
        momentum = price / series.closes[i - period] - 1
        if momentum <= 0:
            continue
        volatility = statistics.stdev(series.returns[i - 19 : i + 1]) * math.sqrt(252)
        score = momentum / max(volatility, 0.05)
        output.append({
            "symbol": symbol,
            "score": round(score, 8),
            "price": price,
            "reason": f"{period}-session momentum {momentum:.2%}; volatility {volatility:.2%}; above SMA{trend}; SPY above SMA120",
        })
    output.sort(key=lambda item: (-item["score"], item["symbol"]))
    result = output[: params["top_n"]]
    market.signals[key] = result
    return result


def rank(bars: dict[str, list[dict]], params: dict | None = None, asof: str | None = None) -> list[dict]:
    """Rank up to top_n symbols using only bars dated on/before asof.

    The caller must exclude incomplete daily candles. Missing SPY, a weak market,
    invalid prices, or insufficient history produces cash (an empty ranking).
    """
    selected = _params(params)
    market = _Market(bars, asof)
    return [dict(item) for item in _rank(market, selected, len(market.days) - 1)]


def _backtest(market: _Market, params: dict, start: int, end: int) -> dict:
    """Independent cash-start segment, with end exclusive and no future reads."""
    cash = _CAPITAL
    positions: dict[str, dict] = {}
    equity = peak = _CAPITAL
    daily_returns = []
    max_drawdown = 0.0
    fills = 0
    stopped = 0
    max_gross = 0.0
    for index in range(start, end):
        day = market.days[index]
        stopped_at_open = set()

        def sell(symbol: str, quantity: int, price: float) -> None:
            nonlocal cash, fills
            cash += quantity * price * (1 - _COST)
            positions[symbol]["qty"] -= quantity
            fills += 1
            if positions[symbol]["qty"] == 0:
                del positions[symbol]

        # An overnight gap breaches an existing protective stop before rebalancing.
        for symbol, position in list(positions.items()):
            row = market.row(symbol, day)
            if row and row["o"] <= position["entry"] * (1 - params["stop_pct"]):
                sell(symbol, position["qty"], row["o"])
                stopped_at_open.add(symbol)
                stopped += 1

        picks = _rank(market, params, index - 1)
        names = {item["symbol"] for item in picks}
        # Match the live engine: retained positions keep their original shares and
        # protective stop. Rank changes exit whole positions, never partial resize.
        for symbol, position in list(positions.items()):
            row = market.row(symbol, day)
            if row is None:
                continue  # No price means no invented executable fill.
            if symbol not in names:
                sell(symbol, position["qty"], row["o"])

        # Budget is nominal capital, not an ever-growing equity allocation. As in
        # live execution, unrealized losses cannot free up cost-basis exposure.
        exposure = sum(
            position["qty"] * max(position["entry"], (market.row(symbol, day) or {"o": position["mark"]})["o"])
            for symbol, position in positions.items()
        )
        available = max(0.0, min(cash, _CAPITAL - exposure) - _CAPITAL * 0.02)
        per_name = _CAPITAL / params["top_n"]

        for item in picks:
            symbol = item["symbol"]
            if len(positions) >= params["top_n"] or available <= 0:
                break
            if symbol in positions or symbol in stopped_at_open:
                continue
            row = market.row(symbol, day)
            if not row or row["o"] < 10:
                continue
            quantity = math.floor(min(per_name, available, max(0.0, _CAPITAL - exposure)) / (row["o"] * (1 + _COST)))
            if quantity <= 0:
                continue
            spent = quantity * row["o"] * (1 + _COST)
            cash -= spent
            available -= spent
            exposure += quantity * row["o"]
            positions[symbol] = {"qty": quantity, "entry": row["o"], "mark": row["o"]}
            fills += 1

        gross = sum(position["qty"] * (market.row(symbol, day) or {"o": position["mark"]})["o"] for symbol, position in positions.items())
        max_gross = max(max_gross, gross / max(cash + gross, 0.01))
        for symbol, position in list(positions.items()):
            row = market.row(symbol, day)
            if not row:
                continue
            stop = position["entry"] * (1 - params["stop_pct"])
            if row["l"] <= stop:
                sell(symbol, position["qty"], min(row["o"], stop))
                stopped += 1
            else:
                position["mark"] = row["c"]
        next_equity = cash + sum(position["qty"] * position["mark"] for position in positions.values())
        # Charge an exit fee at each segment end so open positions cannot hide costs.
        if index == end - 1:
            next_equity -= sum(position["qty"] * position["mark"] * _COST for position in positions.values())
        daily_returns.append(next_equity / equity - 1)
        equity = next_equity
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1 - equity / peak)
    deviation = statistics.stdev(daily_returns) if len(daily_returns) > 1 else 0.0
    sharpe = statistics.mean(daily_returns) / deviation * math.sqrt(252) if deviation > 1e-12 else 0.0
    return {
        "total_return": round(equity / _CAPITAL - 1, 8),
        "max_drawdown": round(max_drawdown, 8),
        "sharpe": round(sharpe, 6),
        "final_equity": round(equity, 2),
        "sessions": end - start,
        "fills": fills,
        "stops": stopped,
        "max_gross_exposure": round(max_gross, 8),
        "start": market.days[start] if start < len(market.days) else None,
        "end": market.days[end - 1] if end > start else None,
    }


def _objective(metrics: dict) -> float:
    return metrics["total_return"] - 2 * metrics["max_drawdown"]


def research(bars: dict[str, list[dict]], incumbent: dict | None = None) -> dict:
    """Select on chronological train/validation; holdout only confirms or rejects.

    Returned params are always the parameters callers should retain/use. A false
    recommend_promote keeps the incumbent (or defaults on the first run). Only
    12 bounded candidates exist; account allocation and stops are never evolved.
    All returns and drawdowns in metrics are fractions, not percentages.
    """
    current = _params(incumbent)
    market = _Market(bars)
    output = {
        "params": current,
        "recommend_promote": False,
        "selected_name": _name(current),
        "metrics": {},
        "validation": {"required_sessions": _MIN_SESSIONS, "available_sessions": len(market.days), "passed": False},
        "reason": "Insufficient completed SPY sessions; retain incumbent/default and collect history.",
        "rankings": rank(bars, current),
        "limitations": list(LIMITATIONS),
    }
    if len(market.days) < _MIN_SESSIONS:
        return output
    remaining = len(market.days) - _WARMUP
    train_end = _WARMUP + remaining // 2
    validation_end = train_end + (remaining - remaining // 2) // 2
    segments = {
        "training": (_WARMUP, train_end),
        "validation": (train_end, validation_end),
        "holdout": (validation_end, len(market.days)),
    }
    candidates = [
        {"momentum": momentum, "trend": trend, "top_n": top_n, "stop_pct": 0.05}
        for momentum, trend, top_n in itertools.product(_ALLOWED["momentum"], _ALLOWED["trend"], _ALLOWED["top_n"])
    ]
    training = {_name(p): _backtest(market, p, *segments["training"]) for p in candidates}
    ordered = sorted(candidates, key=lambda p: (-_objective(training[_name(p)]), _name(p)))
    shortlist = ordered[:3]
    if current not in shortlist:
        shortlist.append(current)
    validation = {_name(p): _backtest(market, p, *segments["validation"]) for p in shortlist}
    # Freeze the nominee before ever evaluating the final holdout.
    nominee = min(shortlist, key=lambda p: (
        -(0.3 * _objective(training[_name(p)]) + 0.7 * _objective(validation[_name(p)])), _name(p)
    ))
    nominee_name, current_name = _name(nominee), _name(current)
    candidate_holdout = _backtest(market, nominee, *segments["holdout"])
    incumbent_holdout = candidate_holdout if nominee == current else _backtest(market, current, *segments["holdout"])
    valid = validation[nominee_name]
    validation_pass = valid["total_return"] > 0 and valid["max_drawdown"] <= _MAX_DRAWDOWN and valid["fills"] > 0
    holdout_pass = candidate_holdout["total_return"] > 0 and candidate_holdout["max_drawdown"] <= _MAX_DRAWDOWN and candidate_holdout["fills"] > 0
    improvement_pass = nominee == current or candidate_holdout["total_return"] >= incumbent_holdout["total_return"] + _IMPROVEMENT
    promote = validation_pass and holdout_pass and improvement_pass and (nominee != current or incumbent is None)
    chosen = dict(nominee if promote else current)
    if promote:
        reason = "Frozen candidate passed validation and holdout; adopt bounded strategy parameters."
    elif not validation_pass:
        reason = "Candidate failed positive-return/8% drawdown validation gate; retain incumbent/default."
    elif not holdout_pass:
        reason = "Candidate failed positive-return/8% drawdown holdout gate; retain incumbent/default."
    elif not improvement_pass:
        reason = "Candidate did not beat incumbent holdout return by 25 bps; retain incumbent/default."
    else:
        reason = "Incumbent remains the selected strategy; no parameter change."
    output.update({
        "params": chosen,
        "recommend_promote": bool(promote),
        "selected_name": _name(chosen),
        "metrics": {
            "candidate": {"training": training[nominee_name], "validation": valid, "holdout": candidate_holdout},
            "incumbent": {"training": training[current_name], "validation": validation[current_name], "holdout": incumbent_holdout},
        },
        "validation": {
            "required_sessions": _MIN_SESSIONS,
            "available_sessions": len(market.days),
            "passed": bool(validation_pass and holdout_pass and improvement_pass),
            "candidate_count": len(candidates),
            "shortlist": [_name(p) for p in shortlist],
            "nominated_name": nominee_name,
            "nominated_params": nominee,
            "validation_pass": bool(validation_pass),
            "holdout_pass": bool(holdout_pass),
            "improvement_pass": bool(improvement_pass),
            "minimum_holdout_improvement": _IMPROVEMENT,
            "maximum_drawdown": _MAX_DRAWDOWN,
            "segments": {name: {"start": market.days[start], "end": market.days[end - 1], "sessions": end - start} for name, (start, end) in segments.items()},
            "protocol": "Training ranks 12 candidates; top three plus incumbent enter validation; one frozen nominee and incumbent enter holdout. Every segment starts in cash.",
        },
        "reason": reason,
        "rankings": [dict(item) for item in _rank(market, chosen, len(market.days) - 1)],
    })
    return output
