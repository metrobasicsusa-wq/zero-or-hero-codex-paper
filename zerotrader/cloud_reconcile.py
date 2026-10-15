"""GET-only pre-enable reconciliation; no engine, cycle, state commit or adoption.

Restore a pinned complete private bundle into disposable private storage, compare
PAPER observations in memory, then confirm the same remote HEAD. Broker reads are
not atomic; matching beginning/end inventory observations cannot prove continuous
stop coverage or exclude intervening activity. FILL availability is checked, not
archived here. Passing this command neither enables trading nor grants a lease.
"""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import urllib.error
import urllib.parse
import urllib.request

from .broker import PaperBroker, BrokerError
from .cloud_auxiliary import restore_auxiliary
from .cloud_bundle import GitBundleStore
from .cloud_checkpoint import _canonical, _check_state
from .cloud_github import PrivateGithubCheckpointStore, normalize_secret_text
from .cloud_preflight import CODE_REPOSITORY, _secret
from .cloud_runner import account_digest, validate_cutover
from .data import NY, timestamp
from .engine import TERMINAL, flatten
from .risk import validate_config
from .storage import ROOT, read_json

_HASH = re.compile(r"[0-9a-f]{64}\Z")
_OID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_OCC = re.compile(r"([A-Z]{1,6})([0-9]{6})([CP])([0-9]{8})\Z")
_MAX_RESPONSE = 4 * 1024 * 1024
_EPSILON = Decimal("0.00000001")
_ERRORS = frozenset({"execution_context_invalid", "credentials_or_binding_invalid", "private_runtime_invalid",
    "checkpoint_mismatch", "restore_or_cutover_invalid", "account_mismatch", "broker_read_failed",
    "broker_response_invalid", "order_identity_mismatch", "unresolved_intents", "unknown_activity",
    "position_quantity_mismatch", "protective_coverage_missing", "risk_or_account_blocks_enable",
    "broker_changed_during_read", "remote_state_changed", "unexpected_failure", "invalid_arguments"})


class ReconciliationFailure(RuntimeError):
    def __init__(self, category):
        self.category = category if category in _ERRORS else "unexpected_failure"
        super().__init__(self.category)


def _result():
    return {"format_version": 1, "status": "failed", "error_category": None,
        "trading_enabled": False, "broker_writes_performed": False, "remote_writes_performed": False,
        "broker_reads_atomic": False, "continuous_protection_proven": False, "execution_lease_proven": False,
        "fill_evidence_persisted": False, "account_binding_match": False, "expected_head_match": False,
        "complete_archive_restored": False, "cutover_proof_verified": False, "state_unchanged": False,
        "remote_head_unchanged": False, "broker_inventory_stable": False, "fill_backfill_available": False,
        "risk_latched": False, "paused": False, "positions": 0, "open_orders": 0,
        "queried_intents": 0, "unresolved_intents": 0, "unknown_positions": 0, "unknown_orders": 0,
        "quantity_mismatches": 0, "unprotected_positions": 0, "fill_activity_versions": 0,
        "unknown_fill_orders": 0, "overnight_intraday_positions": 0, "reconciliation_passed": False,
        "risk_check_failures": [], "valuation_above_entry_budget": False}


def _allowed_request(request):
    parsed = urllib.parse.urlsplit(request.full_url)
    allowed = {"/v2/account": set(), "/v2/clock": set(), "/v2/positions": set(),
        "/v2/orders": {"status", "nested", "limit", "direction"},
        "/v2/orders:by_client_order_id": {"client_order_id"},
        "/v2/account/activities/FILL": {"after", "until", "page_token", "page_size", "direction"}}
    path = parsed.path
    if path.startswith("/v2/orders/") and _ID.fullmatch(path[len("/v2/orders/"):]):
        names = {"nested"}
    else:
        names = allowed.get(path)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if (request.get_method() != "GET" or request.data is not None
            or parsed.scheme != "https" or parsed.netloc != "paper-api.alpaca.markets"
            or parsed.fragment or names is None or not set(query).issubset(names)
            or any(len(values) != 1 for values in query.values())):
        raise BrokerError("Only scoped PAPER reconciliation GETs are permitted.")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _http_get(request, timeout):
    _allowed_request(request)
    try:
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
            if response.geturl() != request.full_url:
                raise BrokerError("Redirect refused.")
            raw = response.read(_MAX_RESPONSE + 1)
            if len(raw) > _MAX_RESPONSE:
                raise BrokerError("Response capacity exceeded.")
            return response.status, raw
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        return status, b""


class ReadOnlyPaperBroker(PaperBroker):
    def __init__(self, *, credential_loader, transport=None):
        target = transport or _http_get

        def guarded(request, timeout):
            _allowed_request(request)
            status, raw = target(request, timeout)
            if type(status) is not int or not isinstance(raw, bytes) or len(raw) > _MAX_RESPONSE:
                raise BrokerError("Invalid bounded response.")
            return status, raw

        super().__init__(credential_loader=credential_loader, transport=guarded)

    def _request(self, method, path, *, data=False, params=None, payload=None):
        if method != "GET" or data or payload is not None:
            raise BrokerError("Broker mutations are disabled for reconciliation.")
        return super()._request(method, path, data=False, params=params)


class ReadOnlyPrivateStore(PrivateGithubCheckpointStore):
    def _write(self, *args, **kwargs):
        raise ReconciliationFailure("remote_state_changed")


def _number(value, *, positive=False):
    try:
        result = Decimal(str(value))
        if isinstance(value, bool) or not result.is_finite() or result < 0 or (positive and result == 0):
            raise ValueError
        return result
    except (ValueError, TypeError, InvalidOperation):
        raise ReconciliationFailure("broker_response_invalid") from None


def _option_terms(symbol):
    """Only standard, unadjusted OCC contracts are within this pilot."""
    match = _OCC.fullmatch(symbol) if isinstance(symbol, str) else None
    try:
        if match is None or int(match[4]) <= 0:
            raise ValueError
        compact = match[2]
        expiry = datetime(2000 + int(compact[:2]), int(compact[2:4]), int(compact[4:])).date().isoformat()
    except ValueError:
        raise ReconciliationFailure("broker_response_invalid") from None
    return match[1], expiry


def _asset_class(row):
    asset = row.get("asset_class", "us_equity")
    if asset == "us_option":
        _option_terms(row.get("symbol"))
        if "multiplier" in row and _number(row["multiplier"]) != 100:
            raise ReconciliationFailure("broker_response_invalid")
    elif asset != "us_equity" or _OCC.fullmatch(str(row.get("symbol", ""))):
        # Never silently value an option contract as a single stock share.
        raise ReconciliationFailure("broker_response_invalid")
    return asset


def _option_owner(symbol, owner, intent):
    underlying, expiry = _option_terms(symbol)
    contract = owner.get("contract", {})
    match = _OCC.fullmatch(symbol)
    if (owner.get("asset_class") != "us_option" or _number(owner.get("multiplier")) != 100
            or intent.get("asset_class") != "us_option" or _number(intent.get("multiplier")) != 100
            or _number(intent["payload"].get("qty")) != 1
            or not isinstance(contract, dict) or contract.get("symbol") != symbol
            or contract.get("underlying_symbol") != underlying or contract.get("expiration_date") != expiry
            or _number(contract.get("size")) != 100
            or _number(contract.get("strike_price")) != Decimal(match[4]) / 1000
            or contract.get("type") != ("call" if match[3] == "C" else "put")
            or intent.get("contract") != contract):
        raise ReconciliationFailure("order_identity_mismatch")
    return expiry


def _order_map(rows):
    result = {}
    for row in flatten(rows):
        if (not isinstance(row, dict) or not _ID.fullmatch(str(row.get("id", "")))
                or row.get("side") not in ("buy", "sell") or not isinstance(row.get("symbol"), str)
                or not isinstance(row.get("status"), str)):
            raise ReconciliationFailure("broker_response_invalid")
        asset = _asset_class(row)
        qty, filled = _number(row.get("qty"), positive=True), _number(row.get("filled_qty") or 0)
        if (filled > qty or (asset == "us_option" and (qty != 1 or filled not in (0, 1)))
                or (row["id"] in result and result[row["id"]] != row)):
            raise ReconciliationFailure("broker_response_invalid")
        result[row["id"]] = row
    return result


def _inventory_key(positions, orders):
    # Ignore mark-to-market movement, but retain quantities and executable terms.
    fields = ("id", "client_order_id", "symbol", "side", "type", "status", "qty", "filled_qty",
              "limit_price", "stop_price", "order_class", "position_intent", "asset_class", "multiplier")
    order_map = _order_map(orders)
    return (sorted((p["symbol"], str(_number(p["qty"])), _asset_class(p), str(p.get("multiplier"))) for p in positions),
            sorted(tuple(str(order_map[key].get(field)) for field in fields) for key in order_map))


def compare_observations(state, broker, config, cursor, expected_account, result, *, now):
    """Pure comparison: do not reconcile into or save the supplied state."""
    option_config = config.get("options", {})
    options_enabled = isinstance(option_config, dict) and option_config.get("enabled") is True
    options_present = isinstance(option_config, dict) and bool(option_config)
    sleeves = ("swing", "intraday", "options") if options_present else ("swing", "intraday")
    if options_present:
        if (_number(option_config.get("allocation"), positive=True) > 1000
                or sum((_number(config[s]["allocation"], positive=True) for s in sleeves), Decimal(0)) > _number(config["capital"])):
            raise ReconciliationFailure("risk_or_account_blocks_enable")
    # Reserved contingency, not a claim that the broker charged this amount.
    option_fee = _number(option_config.get("fee_allowance_per_contract", 10), positive=True) if options_present else Decimal(0)
    account, clock = broker.account(), broker.clock()
    if not hmac.compare_digest(hashlib.sha256(str(account.get("id", "")).encode()).hexdigest(), expected_account):
        raise ReconciliationFailure("account_mismatch")
    result["account_binding_match"] = True
    observed = timestamp(clock["timestamp"])
    if observed.tzinfo is None or abs((now - observed).total_seconds()) > 120:
        raise ReconciliationFailure("broker_response_invalid")
    equity, cash = _number(account["equity"], positive=True), _number(account["cash"])
    positions, open_rows = broker.positions(), broker.orders()
    if len({p.get("symbol") for p in positions}) != len(positions):
        raise ReconciliationFailure("broker_response_invalid")
    open_orders = _order_map(open_rows)
    result.update(positions=len(positions), open_orders=len(open_orders),
                  risk_latched=bool(state["halted"]), paused=state["paused"])
    initial_key = _inventory_key(positions, open_rows)
    observed_orders, lineage = {}, {}
    fills = deepcopy(state["fills"])
    known = set(state.get("known_order_ids", [])) | set(fills)
    for symbol, owner in state["owners"].items():
        intent = state["intents"].get(owner.get("entry_cid"))
        if (owner.get("sleeve") not in sleeves or intent is None
                or intent["sleeve"] != owner["sleeve"] or intent["payload"].get("symbol") != symbol
                or intent["payload"].get("side") != "buy"):
            raise ReconciliationFailure("order_identity_mismatch")
        if owner["sleeve"] == "options":
            _option_owner(symbol, owner, intent)
    entry_ids = {owner["entry_cid"] for owner in state["owners"].values()}
    for cid, intent in state["intents"].items():
        if intent.get("order_id"):
            known.add(intent["order_id"])
        if intent.get("status") in TERMINAL and cid not in entry_ids:
            continue
        result["queried_intents"] += 1
        if result["queried_intents"] > 2000:
            raise ReconciliationFailure("broker_response_invalid")
        order = (broker.get_order(intent["order_id"]) if intent.get("order_id")
                 else broker.order_by_client_id(cid))
        if order is None:
            result["unresolved_intents"] += 1
            continue
        payload = intent["payload"]
        if (order.get("client_order_id") != cid or order.get("symbol") != payload["symbol"]
                or order.get("side") != payload["side"] or _number(order.get("qty")) != _number(payload["qty"])
                or (intent.get("order_id") and order.get("id") != intent["order_id"])
                or (payload.get("limit_price") is not None
                    and _number(order.get("limit_price")) != _number(payload["limit_price"]))):
            raise ReconciliationFailure("order_identity_mismatch")
        if intent.get("sleeve") == "options":
            if (not options_present or _asset_class(order) != "us_option"
                    or intent.get("asset_class") != "us_option" or _number(intent.get("multiplier")) != 100
                    or _number(payload.get("qty")) != 1
                    or order.get("position_intent") != ("buy_to_open" if payload["side"] == "buy" else "sell_to_close")):
                raise ReconciliationFailure("order_identity_mismatch")
        elif _asset_class(order) != "us_equity":
            raise ReconciliationFailure("order_identity_mismatch")
        children = _order_map([order])
        if any(row["status"] == "replaced" for row in children.values()):
            result["unresolved_intents"] += 1
        parent_entry = cid if payload["side"] == "buy" else intent.get("exit_for")
        for oid, row in children.items():
            if row["symbol"] != payload["symbol"]:
                raise ReconciliationFailure("order_identity_mismatch")
            if row.get("type") == "stop" and payload.get("stop_loss"):
                if _number(row.get("stop_price")) != _number(payload["stop_loss"].get("stop_price")):
                    raise ReconciliationFailure("order_identity_mismatch")
            known.add(oid)
            observed_orders[oid] = row
            lineage[oid] = parent_entry
            fills[oid] = {"symbol": row["symbol"], "side": row["side"],
                          "qty": str(_number(row.get("filled_qty") or 0)), "sleeve": intent["sleeve"]}
    for oid, order in open_orders.items():
        owner = state["owners"].get(order["symbol"])
        if not owner or lineage.get(oid) != owner["entry_cid"]:
            result["unknown_orders"] += 1
        if oid in observed_orders and _inventory_key([], [order]) != _inventory_key([], [observed_orders[oid]]):
            raise ReconciliationFailure("broker_changed_during_read")
    expected_qty = {}
    for fill in fills.values():
        if fill.get("side") not in ("buy", "sell"):
            raise ReconciliationFailure("broker_response_invalid")
        symbol = fill["symbol"]
        expected_qty[symbol] = expected_qty.get(symbol, Decimal(0)) + _number(fill["qty"]) * (1 if fill["side"] == "buy" else -1)
    actual_qty, gross = {}, Decimal(0)
    exposures = {sleeve: Decimal(0) for sleeve in sleeves}
    acquisition = {sleeve: Decimal(0) for sleeve in exposures}
    for position in positions:
        symbol, qty = position["symbol"], _number(position["qty"], positive=True)
        asset = _asset_class(position)
        if asset == "us_option" and (not options_present or qty != 1):
            raise ReconciliationFailure("broker_response_invalid")
        actual_qty[symbol] = qty
        owner = state["owners"].get(symbol)
        if not owner:
            result["unknown_positions"] += 1
            continue
        if (asset == "us_option") != (owner["sleeve"] == "options"):
            raise ReconciliationFailure("order_identity_mismatch")
        cost = _number(position["cost_basis"])
        if asset == "us_option":
            expiry = _option_owner(symbol, owner, state["intents"][owner["entry_cid"]])
            if (expiry < observed.astimezone(NY).date().isoformat()
                    or ("expiration_date" in position and position["expiration_date"] != expiry)):
                raise ReconciliationFailure("order_identity_mismatch")
            cost += option_fee
        value = max(_number(position["market_value"]), cost)
        gross += value
        exposures[owner["sleeve"]] += value
        acquisition[owner["sleeve"]] += cost
        if owner["sleeve"] == "intraday" and owner.get("entry_day") != observed.astimezone(NY).date().isoformat():
            result["overnight_intraday_positions"] += 1
        if asset == "us_option":
            # DNE is available only on expiry day. Ordinary same-session pilot
            # inventory does not require it. Inventory retained overnight or
            # within the mandatory expiry-exit window must still fail this
            # read-only check, even if a DNE acknowledgement exists. Passing is
            # not proof of a broker stop or continuous option-price protection.
            session_day = observed.astimezone(NY).date()
            expiry_day = datetime.fromisoformat(expiry).date()
            session_exit_due = True
            if clock.get("is_open") is True:
                try:
                    close = timestamp(clock["next_close"])
                except (KeyError, ValueError, TypeError, AttributeError):
                    raise ReconciliationFailure("broker_response_invalid") from None
                if (close.tzinfo is None or close.astimezone(NY).date() != session_day or close <= observed):
                    raise ReconciliationFailure("broker_response_invalid")
                # Use the broker's session close, including shortened sessions.
                session_exit_due = observed >= close - timedelta(minutes=20)
            if (owner.get("entry_day") != session_day.isoformat()
                    or session_exit_due
                    or expiry_day - session_day <= timedelta(days=2)
                    or (expiry_day == session_day and owner.get("dne_status") != "acknowledged")):
                result["unprotected_positions"] += 1
            continue
        coverage = sum((_number(o["qty"]) - _number(o.get("filled_qty") or 0) for oid, o in open_orders.items()
                        if lineage.get(oid) == owner["entry_cid"] and o["symbol"] == symbol and o["side"] == "sell"
                        and o.get("type") == "stop" and o["status"] in ("new", "accepted", "accepted_for_bidding")
                        and _number(o.get("stop_price"), positive=True) > 0), Decimal(0))
        if coverage + _EPSILON < qty:
            result["unprotected_positions"] += 1
    result["quantity_mismatches"] = sum(abs(actual_qty.get(symbol, Decimal(0)) - expected_qty.get(symbol, Decimal(0))) > _EPSILON
                                       for symbol in set(actual_qty) | set(expected_qty))
    after = None
    if cursor.get("last_successful_poll"):
        last = timestamp(cursor["last_successful_poll"])
        if last.tzinfo is None or last > observed:
            raise ReconciliationFailure("broker_response_invalid")
        after = (last - timedelta(days=1)).isoformat()
    activities = broker.fill_activities(after=after, until=observed.isoformat())
    known_terms = {oid: (fill["symbol"], fill["side"]) for oid, fill in fills.items()}
    for intent in state["intents"].values():
        if intent.get("order_id"):
            known_terms.setdefault(intent["order_id"], (intent["payload"]["symbol"], intent["payload"]["side"]))
    seen = set()
    for fill in activities:
        if (fill.get("activity_type") != "FILL" or not isinstance(fill.get("id"), str)
                or not fill["id"] or fill["id"] in seen or fill.get("side") not in ("buy", "sell")
                or timestamp(fill["transaction_time"]).tzinfo is None):
            raise ReconciliationFailure("broker_response_invalid")
        seen.add(fill["id"])
        fill_qty = _number(fill["qty"], positive=True)
        _number(fill["price"], positive=True)
        if _OCC.fullmatch(str(fill.get("symbol", ""))):
            _option_terms(fill["symbol"])
            if fill_qty != 1 or fill.get("asset_class", "us_option") != "us_option":
                raise ReconciliationFailure("broker_response_invalid")
        if (fill.get("order_id") not in known
                or known_terms.get(fill.get("order_id")) != (fill.get("symbol"), fill["side"])):
            result["unknown_fill_orders"] += 1
    result.update(fill_activity_versions=len(seen), fill_backfill_available=True)
    ending_positions, ending_orders = broker.positions(), broker.orders()
    if initial_key != _inventory_key(ending_positions, ending_orders):
        raise ReconciliationFailure("broker_changed_during_read")
    result["broker_inventory_stable"] = True
    if result["unresolved_intents"]:
        raise ReconciliationFailure("unresolved_intents")
    if result["unknown_positions"] or result["unknown_orders"] or result["unknown_fill_orders"]:
        raise ReconciliationFailure("unknown_activity")
    if result["quantity_mismatches"]:
        raise ReconciliationFailure("position_quantity_mismatch")
    if result["unprotected_positions"]:
        raise ReconciliationFailure("protective_coverage_missing")
    reservations = {sleeve: Decimal(0) for sleeve in exposures}
    for oid, order in open_orders.items():
        if order["side"] != "buy":
            continue
        if order.get("type") != "limit":
            raise ReconciliationFailure("broker_response_invalid")
        remaining = _number(order["qty"]) - _number(order.get("filled_qty") or 0)
        reserved = remaining * _number(order.get("limit_price"), positive=True)
        if _asset_class(order) == "us_option":
            reserved = reserved * 100 + (option_fee if remaining else Decimal(0))
        reservations[state["owners"][order["symbol"]]["sleeve"]] += reserved
    total_reserved = sum(reservations.values(), Decimal(0))
    peak = max(_number(state["equity_peak"], positive=True), equity)
    # Engine.cycle applies max(market value, cost basis) to NEW entries; an
    # already-owned position appreciating above its entry budget does not halt
    # protective management or authorize additional investment. Match that
    # existing behavior during transfer, without changing the engine or caps.
    # Acquisition basis and every pending buy remain bounded independently.
    capital = _number(config["capital"])
    result["valuation_above_entry_budget"] = bool(gross > capital or any(
        exposures[s] > _number(config[s]["allocation"]) for s in exposures))
    checks = {
        "paused_or_halted": bool(state["paused"] or state["halted"]),
        "broker_account_blocked": bool(account.get("status") != "ACTIVE" or account.get("trading_blocked") or account.get("account_blocked")),
        "drawdown_limit": (peak - equity) / peak >= Decimal(str(config["max_drawdown"])) - _EPSILON,
        "acquisition_capital_limit": sum(acquisition.values(), Decimal(0)) + total_reserved > capital,
        "acquisition_sleeve_limit": any(acquisition[s] + reservations[s] > _number(config[s]["allocation"]) for s in exposures),
        "pending_buy_capital_limit": total_reserved > 0 and gross + total_reserved > capital,
        "pending_buy_sleeve_limit": any(reservations[s] > 0 and exposures[s] + reservations[s] > _number(config[s]["allocation"]) for s in exposures),
        "pending_buy_cash_limit": total_reserved > cash,
        "cash_reserve_limit": total_reserved > 0 and cash - total_reserved < capital * _number(config["cash_reserve_fraction"]),
        "overnight_intraday": bool(result["overnight_intraday_positions"]),
        "options_entries_disabled": bool(not options_enabled and reservations.get("options", 0) > 0),
    }
    result["risk_check_failures"] = sorted(name for name, failed in checks.items() if failed)
    if result["risk_check_failures"]:
        raise ReconciliationFailure("risk_or_account_blocks_enable")


def run_reconciliation(*, environ=None, core_factory=ReadOnlyPrivateStore, bundle_factory=GitBundleStore,
                       paper_transport=None, now=None):
    result, state, original = _result(), None, None
    env = os.environ if environ is None else environ
    try:
        if (env.get("GITHUB_ACTIONS") != "true" or env.get("GITHUB_REPOSITORY") != CODE_REPOSITORY
                or env.get("GITHUB_REF") != "refs/heads/main" or env.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
                or env.get("ZOH_CLOUD_ENABLED", "false") != "false"):
            raise ReconciliationFailure("execution_context_invalid")
        key, secret, token, expected, head = (normalize_secret_text(env.get(name, "")) for name in
            ("ZOH_MAIN_PAPER_KEY", "ZOH_MAIN_PAPER_SECRET", "ZOH_STATE_GITHUB_TOKEN",
             "ZOH_EXPECTED_ACCOUNT_SHA256", "ZOH_EXPECTED_CORE_COMMIT"))
        if (not _secret(key) or not _secret(secret) or not _secret(token, minimum=20)
                or not isinstance(expected, str) or not _HASH.fullmatch(expected)
                or not isinstance(head, str) or not _OID.fullmatch(head)):
            raise ReconciliationFailure("credentials_or_binding_invalid")
        temporary_root, workspace = Path(env.get("RUNNER_TEMP", "")), Path(env.get("GITHUB_WORKSPACE", ""))
        if (not temporary_root.is_absolute() or not temporary_root.is_dir() or not workspace.is_absolute()
                or temporary_root.resolve().is_relative_to(workspace.resolve())):
            raise ReconciliationFailure("private_runtime_invalid")
        config = read_json(ROOT / "settings.json")
        validate_config(config)
        core = core_factory(credential_loader=lambda: token, branch="core-checkpoints")
        try:
            first = core.load()
            if first.receipt.commit != head:
                raise ReconciliationFailure("checkpoint_mismatch")
            with tempfile.TemporaryDirectory(prefix="zoh-private-reconcile-", dir=temporary_root) as temporary:
                directory = Path(temporary)
                directory.chmod(0o700)
                archive_path = directory / "archive.sqlite3"
                with bundle_factory(core) as bundle:
                    loaded = bundle.restore(archive_path)
                state = loaded.state
                original = _canonical(state)
                _check_state(state)
                if (loaded.receipt.commit != head or loaded.receipt.state_sha256 != hashlib.sha256(original).hexdigest()
                        or not hmac.compare_digest(account_digest(state), expected)):
                    raise ReconciliationFailure("checkpoint_mismatch")
                result.update(expected_head_match=True, complete_archive_restored=True)
                restore_auxiliary(archive_path, directory)
                proof = read_json(directory / "cloud-cutover.json")
                validate_cutover(proof, state, expected, now=now)
                with closing(sqlite3.connect(archive_path.as_uri() + "?mode=ro", uri=True)) as db:
                    if db.execute("SELECT COALESCE(MAX(sequence),0) FROM events").fetchone()[0] < proof["evidence_cutoff"]:
                        raise ReconciliationFailure("restore_or_cutover_invalid")
                result["cutover_proof_verified"] = True
                cursor = read_json(directory / "fill-archive-cursor.json", {})
                broker = ReadOnlyPaperBroker(credential_loader=lambda: {"key": key, "secret": secret}, transport=paper_transport)
                compare_observations(state, broker, config, cursor, expected, result, now=now or datetime.now(timezone.utc))
                try:
                    confirmed = core.confirm(loaded.receipt.checkpoint)
                except Exception:
                    raise ReconciliationFailure("remote_state_changed") from None
                if confirmed.receipt.commit != head or _canonical(confirmed.state) != original:
                    raise ReconciliationFailure("remote_state_changed")
                result.update(remote_head_unchanged=True, reconciliation_passed=True, status="passed_readonly_reconciliation")
        finally:
            core.close()
    except ReconciliationFailure as error:
        result["error_category"] = error.category
    except BrokerError:
        result["error_category"] = "broker_read_failed"
    except Exception:
        result["error_category"] = "unexpected_failure"
    finally:
        if state is not None and original is not None:
            try:
                result["state_unchanged"] = _canonical(state) == original
            except Exception:
                result["state_unchanged"] = False
            if not result["state_unchanged"]:
                result.update(status="failed", reconciliation_passed=False, error_category="remote_state_changed")
    if result["error_category"] is not None:
        result.update(status="failed", reconciliation_passed=False)
    return result


def main(argv=None):
    import sys
    args = sys.argv[1:] if argv is None else argv
    if args:
        result = _result()
        result["error_category"] = "invalid_arguments"
    else:
        result = run_reconciliation()
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0 if result["reconciliation_passed"] else 1
