"""Pure, fail-closed policy for the limited PAPER indicative-options pilot.

No broker, filesystem or network access occurs here. Money is validated and
calculated with Decimal; public return values are JSON-safe. Cumulative order
observations reserve funds only and NEVER constitute independent FILL statistics.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_UP
import re
from zoneinfo import ZoneInfo


NY = ZoneInfo("America/New_York")
OCC = re.compile(r"^([A-Z]{1,6})([0-9]{6})([CP])([0-9]{8})$")
LIMITATIONS = [
    "Indicative free option quotes are derivative reference prices, not executable OPRA quotes.",
    "Paper execution does not establish real option liquidity, queue priority or achievable returns.",
    "This is an unvalidated call-only pilot; stock signals do not validate an options strategy.",
]
DEFAULT_POLICY = {
    "enabled": False, "allocation": 1000, "lifetime_premium_cap": 960,
    "fee_contingency": 40, "max_lifetime_contracts": 4,
    "max_premium_per_contract": 240, "fee_allowance_per_contract": 10,
    "max_positions": 2, "quote_feed": "indicative",
    "max_quote_age_seconds": 90, "max_spread_fraction": .10,
    "min_dte": 7, "max_dte": 21, "max_moneyness_fraction": .01,
}
TERMINAL = {"filled", "canceled", "expired", "rejected", "replaced", "not_submitted"}


def _decimal(value, name, *, minimum=None, maximum=None):
    if isinstance(value, bool) or value is None:
        raise ValueError(f"Invalid {name}.")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"Invalid {name}.") from None
    if not result.is_finite() or (minimum is not None and result < Decimal(str(minimum))) or (
            maximum is not None and result > Decimal(str(maximum))):
        raise ValueError(f"{name} is outside the pilot policy.")
    return result


def _integer(value, name, minimum=0, maximum=None):
    result = _decimal(value, name, minimum=minimum, maximum=maximum)
    if result != result.to_integral_value():
        raise ValueError(f"{name} must be an integer.")
    return int(result)


def _money(value):
    try:
        return format(value.quantize(Decimal(".01"), rounding=ROUND_UP), ".2f")
    except InvalidOperation:
        raise ValueError("Monetary value cannot be represented safely.") from None


def _dt(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        raise ValueError("A valid timestamp is required.") from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Timestamps must include a timezone.")
    return result.astimezone(timezone.utc)


def policy_config(config):
    """Validate optional config['options']; missing means disabled, never enabled.

The optional knobs may only tighten the authorized first-stage bounds. Active
allocation must fit inside the existing capital, with stock intraday <= $49k.
"""
    raw = config.get("options", {})
    if not isinstance(raw, dict) or set(raw) - set(DEFAULT_POLICY):
        raise ValueError("Unknown or malformed options policy keys.")
    p = dict(DEFAULT_POLICY, **raw)
    if type(p["enabled"]) is not bool:
        raise ValueError("Options enabled must be boolean.")
    for key, low, high in (
        ("allocation", .01, 1000), ("lifetime_premium_cap", .01, 960),
        ("fee_contingency", 40, 1000), ("max_premium_per_contract", .01, 240),
        ("fee_allowance_per_contract", 10, 1000),
        ("max_spread_fraction", .000001, .10), ("max_moneyness_fraction", .000001, .01),
    ):
        p[key] = float(_decimal(p[key], key, minimum=low, maximum=high))
    for key, low, high in (
        ("max_lifetime_contracts", 1, 4), ("max_positions", 1, 2),
        ("max_quote_age_seconds", 1, 90), ("min_dte", 7, 21), ("max_dte", 7, 21),
    ):
        p[key] = _integer(p[key], key, low, high)
    if p["quote_feed"] != "indicative" or p["min_dte"] > p["max_dte"]:
        raise ValueError("Only indicative quotes and the bounded expiry window are allowed.")
    if _decimal(p["lifetime_premium_cap"], "premium cap") + _decimal(p["fee_contingency"], "fees") > _decimal(p["allocation"], "allocation"):
        raise ValueError("Premium and fee reserves exceed the options allocation.")
    if _decimal(p["fee_allowance_per_contract"], "fee allowance") * p["max_lifetime_contracts"] > _decimal(p["fee_contingency"], "fee contingency"):
        raise ValueError("Fee contingency cannot cover the lifetime contract allowance.")
    if p["enabled"]:
        if config.get("mode") != "paper":
            raise ValueError("Options pilot requires paper mode.")
        capital = _decimal(config.get("capital"), "capital", minimum=.01, maximum=100000)
        swing = _decimal(config.get("swing", {}).get("allocation"), "swing allocation", minimum=.01, maximum=50000)
        intraday = _decimal(config.get("intraday", {}).get("allocation"), "intraday allocation", minimum=.01, maximum=49000)
        if swing + intraday + _decimal(p["allocation"], "options allocation") > capital:
            raise ValueError("Options must be carved from, not added to, existing capital.")
        _decimal(config.get("max_drawdown"), "drawdown", minimum=.000001, maximum=.10)
    return p


def parse_option_symbol(symbol):
    """Parse a canonical unadjusted OCC symbol; no spaces or adjusted digit roots."""
    if not isinstance(symbol, str) or not (match := OCC.fullmatch(symbol)):
        raise ValueError("A canonical standard OCC option symbol is required.")
    underlying, compact, kind, strike = match.groups()
    try:
        expiry = date(2000 + int(compact[:2]), int(compact[2:4]), int(compact[4:]))
    except ValueError:
        raise ValueError("Invalid OCC expiration date.") from None
    strike_price = Decimal(strike) / 1000
    if strike_price <= 0:
        raise ValueError("Option strike must be positive.")
    return {"symbol": symbol, "underlying_symbol": underlying,
            "expiry": expiry.isoformat(), "type": "call" if kind == "C" else "put",
            "strike": float(strike_price), "multiplier": 100}


def validate_contract(contract, now=None, policy=None):
    """Require explicit standard-100, active, tradable metadata matching OCC."""
    parsed = parse_option_symbol(contract.get("symbol"))
    if contract.get("status") != "active" or contract.get("tradable") is not True:
        raise ValueError("Option contract must explicitly be active and tradable.")
    if contract.get("asset_class", contract.get("class", "us_option")) != "us_option":
        raise ValueError("Contract must be a US option.")
    size = contract.get("size", contract.get("multiplier"))
    if _integer(size, "contract multiplier", 100, 100) != 100:
        raise ValueError("Only standard 100-share contracts are allowed.")
    if "size" in contract and "multiplier" in contract and _integer(contract["multiplier"], "multiplier", 100, 100) != 100:
        raise ValueError("Conflicting contract multiplier.")
    if (contract.get("type") != parsed["type"] or contract.get("underlying_symbol") != parsed["underlying_symbol"]
            or contract.get("expiration_date") != parsed["expiry"]
            or _decimal(contract.get("strike_price"), "strike") != Decimal(str(parsed["strike"]))):
        raise ValueError("Contract metadata does not match OCC symbol.")
    if now is not None:
        p = policy or DEFAULT_POLICY
        dte = (date.fromisoformat(parsed["expiry"]) - _dt(now).astimezone(NY).date()).days
        if not p["min_dte"] <= dte <= p["max_dte"]:
            raise ValueError("Expiry is outside the 7–21 day entry window.")
        parsed["dte"] = dte
    return parsed


def _quote(quote, now, age_limit, spread_limit):
    bid = _decimal(quote.get("bp"), "bid", minimum=.000001, maximum=1000000000000)
    ask = _decimal(quote.get("ap"), "ask", minimum=.000001, maximum=1000000000000)
    if ask < bid:
        raise ValueError("Crossed option quote.")
    stamp = _dt(quote.get("t"))
    age = (_dt(now) - stamp).total_seconds()
    if age < -5 or age > age_limit:
        raise ValueError("Quote is stale or from the future.")
    spread = (ask - bid) / ((ask + bid) / 2)
    if spread > Decimal(str(spread_limit)):
        raise ValueError("Quote spread exceeds the policy threshold.")
    return bid, ask, stamp, age, spread


def validated_option_quote(quote, now, policy=None):
    """Validate explicitly labeled indicative inputs, never relabel them executable."""
    p = policy or DEFAULT_POLICY
    if quote.get("feed") != "indicative" or quote.get("executable") is True:
        raise ValueError("An explicit indicative, non-executable quote is required.")
    bid, ask, stamp, age, spread = _quote(quote, now, p["max_quote_age_seconds"], p["max_spread_fraction"])
    return {"bid": float(bid), "ask": float(ask), "at": stamp.isoformat(),
            "age_seconds": age, "spread_fraction": float(spread), "feed": "indicative",
            "executable": False, "limitations": list(LIMITATIONS)}


def _option_row(row, owner=None):
    symbol = row.get("symbol")
    marked = (row.get("sleeve") == "options" or (owner or {}).get("sleeve") == "options"
              or row.get("asset_class", row.get("class")) == "us_option")
    return marked or (isinstance(symbol, str) and OCC.fullmatch(symbol) is not None)


def reservation_summary(state, positions, config, previous=None):
    """Reserve lifetime buy costs; sales never refill the first-stage allowance.

Read previous from strategies.options.risk_ledger unless explicitly supplied.
Persist the returned risk_ledger after every observation. Per-order maxima never
decrease, including after corrections, deletion of historical intents, terminal
cancellation or sales. Only a never-submitted archive failure with no broker ID
or fill may be excluded before it has ever been reserved. Ambiguous outcomes
remain fully reserved. This deliberately needs a human accounting review before
recycling any rejected/canceled reservation. No actual-FILL counts are returned.
"""
    p = policy_config(config)
    prior = previous if previous is not None else state.get("strategies", {}).get("options", {}).get("risk_ledger", {})
    if not isinstance(prior, dict) or set(prior) - {"version", "orders"} or prior.get("version", 1) != 1:
        raise ValueError("Malformed options risk ledger.")
    ledger = deepcopy(prior.get("orders", {}))
    if not isinstance(ledger, dict):
        raise ValueError("Malformed options order reservations.")
    for cid, entry in ledger.items():
        if not isinstance(cid, str) or not cid or not isinstance(entry, dict):
            raise ValueError("Malformed options reservation.")
        parse_option_symbol(entry.get("symbol"))
        _decimal(entry.get("premium_reserved"), "reserved premium", minimum=0)
        reserved_qty = _integer(entry.get("contracts_reserved"), "reserved contracts", 1)
        _integer(entry.get("filled_contracts_observed", 0), "observed filled contracts", 0, reserved_qty)
        _decimal(entry.get("fee_reserved"), "reserved fees", minimum=reserved_qty * p["fee_allowance_per_contract"])
    intents = state.get("intents", {})
    fills = state.get("fills", {})
    if not isinstance(intents, dict) or not isinstance(fills, dict):
        raise ValueError("Malformed intent or cumulative-observation ledger.")
    orders = {}
    exit_orders = {}
    active_buys = set()
    violations = []
    for cid, intent in intents.items():
        payload = intent.get("payload", {})
        if not _option_row(dict(payload, sleeve=intent.get("sleeve"))):
            continue
        parsed = parse_option_symbol(payload.get("symbol"))
        if intent.get("sleeve") != "options":
            raise ValueError("Option intent has no authorized options owner.")
        if payload.get("side") not in ("buy", "sell"):
            raise ValueError("Invalid option side.")
        qty = _integer(payload.get("qty"), "option order quantity", 1, 1)
        if payload.get("position_intent") != ("buy_to_open" if payload["side"] == "buy" else "sell_to_close"):
            raise ValueError("Only buy-to-open and sell-to-close are authorized.")
        if payload["side"] == "sell":
            if intent.get("order_id") and intent.get("exit_for"):
                exit_orders[intent["order_id"]] = (intent["exit_for"], parsed["symbol"])
            continue
        if parsed["type"] != "call":
            raise ValueError("The first pilot stage only buys calls.")
        if payload.get("type") != "limit":
            raise ValueError("Option entries must be limit orders.")
        limit = _decimal(payload.get("limit_price"), "option limit", minimum=.01)
        if "multiplier" in payload:
            _integer(payload["multiplier"], "order multiplier", 100, 100)
        if intent.get("status") == "not_submitted" and not intent.get("order_id") and cid not in ledger:
            continue
        if limit * 100 > Decimal(str(p["max_premium_per_contract"])):
            violations.append("per_contract_premium_cap_exceeded")
        if intent.get("status") not in TERMINAL:
            active_buys.add(cid)
        if intent.get("order_id"):
            if intent["order_id"] in orders:
                raise ValueError("One broker order is linked to multiple option intents.")
            orders[intent["order_id"]] = cid
        old = ledger.get(cid, {})
        if old and old["symbol"] != parsed["symbol"]:
            raise ValueError("Reservation symbol changed.")
        ledger[cid] = {"symbol": parsed["symbol"],
                       "premium_reserved": _money(max(limit * 100 * qty, _decimal(old.get("premium_reserved", 0), "prior premium"))),
                       "contracts_reserved": max(qty, _integer(old.get("contracts_reserved", 0), "prior contracts")),
                       "filled_contracts_observed": max(qty if intent.get("status") == "filled" else 0,
                                                        _integer(old.get("filled_contracts_observed", 0), "prior filled contracts")),
                       "fee_reserved": _money(max(Decimal(str(p["fee_allowance_per_contract"])) * qty, _decimal(old.get("fee_reserved", 0), "prior fees")))}
    closed = {}
    for order_id, fill in fills.items():
        if not _option_row(fill):
            continue
        parsed = parse_option_symbol(fill.get("symbol"))
        if fill.get("sleeve") != "options" or fill.get("side") not in ("buy", "sell"):
            raise ValueError("Unowned option cumulative observation.")
        qty = _integer(fill.get("qty"), "cumulative option quantity", 0)
        price = _decimal(fill.get("price"), "cumulative option price", minimum=0)
        if qty and price <= 0:
            raise ValueError("Filled option quantity requires a positive price.")
        if fill["side"] == "sell":
            if order_id in exit_orders:
                entry_cid, symbol = exit_orders[order_id]
                if symbol != parsed["symbol"] or entry_cid not in ledger or ledger[entry_cid]["symbol"] != symbol:
                    raise ValueError("Option closing observation has mismatched lineage.")
                closed[entry_cid] = closed.get(entry_cid, 0) + qty
            continue  # proceeds are parked; never offset lifetime expenditure
        cid = orders.get(order_id)
        if cid is None:
            raise ValueError("Option buy observation lacks a reconciled intent.")
        old = ledger[cid]
        if old["symbol"] != parsed["symbol"]:
            raise ValueError("Cumulative observation symbol does not match its order.")
        old["premium_reserved"] = _money(max(_decimal(old["premium_reserved"], "prior premium"), qty * price * 100))
        old["contracts_reserved"] = max(old["contracts_reserved"], qty)
        old["filled_contracts_observed"] = max(old.get("filled_contracts_observed", 0), qty)
        old["fee_reserved"] = _money(max(_decimal(old["fee_reserved"], "prior fees"), qty * Decimal(str(p["fee_allowance_per_contract"]))))
    held_contracts = 0
    held_symbols = set()
    held_by_entry = {}
    owners = state.get("owners", {})
    for position in positions:
        owner = owners.get(position.get("symbol"), {})
        if not _option_row(position, owner):
            continue
        parsed = parse_option_symbol(position.get("symbol"))
        if owner.get("sleeve") != "options" or owner.get("entry_cid") not in ledger:
            raise ValueError("Unowned option position requires manual reconciliation.")
        if position.get("side", "long") != "long":
            raise ValueError("Short option positions are prohibited.")
        _integer(position.get("multiplier", position.get("size")), "position multiplier", 100, 100)
        qty = _integer(position.get("qty"), "option position quantity", 1)
        _decimal(position.get("cost_basis"), "position cost basis", minimum=.01)
        _decimal(position.get("market_value"), "position market value", minimum=0)
        if parsed["symbol"] in held_symbols:
            raise ValueError("Duplicate option position rows.")
        held_symbols.add(parsed["symbol"])
        held_contracts += qty
        held_by_entry[owner["entry_cid"]] = qty
        old = ledger[owner["entry_cid"]]
        if old["symbol"] != parsed["symbol"]:
            raise ValueError("Option position lineage mismatch.")
        old["premium_reserved"] = _money(max(_decimal(old["premium_reserved"], "prior premium"), _decimal(position["cost_basis"], "cost basis")))
        old["contracts_reserved"] = max(old["contracts_reserved"], qty)
        old["filled_contracts_observed"] = max(old.get("filled_contracts_observed", 0), qty)
        old["fee_reserved"] = _money(max(_decimal(old["fee_reserved"], "prior fees"), qty * Decimal(str(p["fee_allowance_per_contract"]))))
    # A terminal fill may precede the broker position snapshot. Only a matched
    # cumulative closing observation can release that occupied slot; an absent
    # row alone is not proof the owned contract was closed.
    active = sum(max(held_by_entry.get(cid, 0),
                     entry["contracts_reserved"] if cid in active_buys else 0,
                     max(0, entry.get("filled_contracts_observed", 0) - closed.get(cid, 0)))
                 for cid, entry in ledger.items())
    premium = sum((_decimal(v["premium_reserved"], "premium") for v in ledger.values()), Decimal(0))
    fees = sum((_decimal(v["fee_reserved"], "fees") for v in ledger.values()), Decimal(0))
    count = sum(v["contracts_reserved"] for v in ledger.values())
    remaining = max(Decimal(0), Decimal(str(p["lifetime_premium_cap"])) - premium)
    reasons = list(dict.fromkeys(violations))
    if any(v["contracts_reserved"] > 1 or _decimal(v["premium_reserved"], "premium") > Decimal(str(p["max_premium_per_contract"])) for v in ledger.values()):
        reasons.append("per_order_contract_or_premium_limit_exceeded")
    if not p["enabled"]:
        reasons.append("options_disabled")
    if premium > Decimal(str(p["lifetime_premium_cap"])) or fees > Decimal(str(p["fee_contingency"])) or premium + fees > Decimal(str(p["allocation"])):
        reasons.append("lifetime_budget_exceeded")
    if remaining < Decimal("1.00"):
        reasons.append("lifetime_premium_exhausted")
    if count >= p["max_lifetime_contracts"]:
        reasons.append("lifetime_contract_allowance_exhausted")
    if active >= p["max_positions"]:
        reasons.append("simultaneous_contract_limit")
    return {"enabled": p["enabled"], "lifetime_premium_reserved": _money(premium),
            "premium_reserved": _money(premium), "contracts_reserved": count,
            "open_positions": len(held_symbols),
            "lifetime_contracts_reserved": count, "fee_reserved": _money(fees),
            "total_reserved": _money(premium + fees), "remaining_premium": _money(remaining),
            "remaining_contracts": max(0, p["max_lifetime_contracts"] - count),
            "active_contracts": active, "available_slots": max(0, p["max_positions"] - active),
            "can_enter": not reasons, "entry_block_reasons": reasons,
            "accounting_source": "conservative_cumulative_order_observations_not_actual_fill_statistics",
            "sale_proceeds_reusable": False, "risk_ledger": {"version": 1, "orders": ledger}}


def select_candidates(signals, contracts, quotes, underlying_quotes, now, config, *, diagnostics=None):
    """Return at most one affordable standard call per current stock signal.

Signals are a list, or {'intraday': list}; no new stock signal is invented.
Each output contains signal provenance and an explicit indicative limitation.
Order/portfolio checks remain the caller's responsibility immediately pre-submit.
Invalid individual candidates are skipped; malformed config raises ValueError.
"""
    p = policy_config(config)
    if not p["enabled"]:
        return []
    rows = signals.get("intraday", []) if isinstance(signals, dict) else signals
    if not isinstance(rows, list):
        raise ValueError("Current intraday signals must be a list.")
    current = _dt(now)
    chosen = []
    for signal in rows:
        try:
            underlying = signal["symbol"]
            if underlying not in config.get("intraday_universe", []):
                continue
            stamp = _dt(signal["signal_time"])
            if not 0 <= (current - stamp).total_seconds() < 900 or stamp.astimezone(NY).date() != current.astimezone(NY).date():
                continue
            if not isinstance(signal.get("reason"), str) or not signal["reason"].strip():
                continue
            _decimal(signal.get("score"), "signal score")
            bid, ask, _, _, _ = _quote(underlying_quotes[underlying], current,
                                      min(p["max_quote_age_seconds"], config.get("max_quote_age_seconds", 90)),
                                      min(.005, config.get("max_spread_fraction", .005)))
            midpoint = (bid + ask) / 2
            eligible = []
            for contract in contracts:
                if contract.get("underlying_symbol") != underlying:
                    continue
                try:
                    parsed = validate_contract(contract, current, p)
                    if parsed["type"] != "call":
                        continue
                    distance = abs(Decimal(str(parsed["strike"])) / midpoint - 1)
                    if distance > Decimal(str(p["max_moneyness_fraction"])):
                        continue
                    quote = validated_option_quote(quotes[parsed["symbol"]], current, p)
                    limit = Decimal(str(quote["ask"])).quantize(Decimal(".01"), rounding=ROUND_UP)
                    premium = limit * 100
                    if premium > Decimal(str(p["max_premium_per_contract"])):
                        if diagnostics is not None:
                            diagnostics.append({'symbol': parsed['symbol'], 'reason': 'premium_above_cap',
                                                'premium': str(premium), 'cap': p['max_premium_per_contract']})
                        continue
                    eligible.append((distance, parsed["dte"], premium, parsed["symbol"], {
                        "symbol": parsed["symbol"], "underlying_symbol": underlying,
                        "expiry": parsed["expiry"], "strike": parsed["strike"], "qty": 1,
                        "multiplier": 100, "limit_price": _money(limit), "premium": _money(premium),
                        "fee_allowance": _money(Decimal(str(p["fee_allowance_per_contract"]))),
                        "signal": deepcopy(signal), "quote": quote, "limitations": list(LIMITATIONS),
                    }))
                except (KeyError, TypeError, ValueError, AttributeError) as exc:
                    if diagnostics is not None:
                        diagnostics.append({'symbol': contract.get('symbol') if isinstance(contract, dict) else None,
                                            'phase': 'contract_quote_validation',
                                            'reason_type': type(exc).__name__, 'reason': str(exc)[:500]})
                    continue
            if eligible:
                chosen.append(min(eligible, key=lambda item: item[:4])[-1])
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            if diagnostics is not None:
                diagnostics.append({'phase': 'underlying_signal_validation',
                                    'reason_type': type(exc).__name__, 'reason': str(exc)[:500]})
            continue
    unique = {}
    for candidate in chosen:
        unique.setdefault(candidate["symbol"], candidate)
    return list(unique.values())
