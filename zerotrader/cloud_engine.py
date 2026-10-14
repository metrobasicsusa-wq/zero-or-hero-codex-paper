"""Opt-in cloud execution adapter; no runner or live deployment imports this.

The coordinator must supply exclusive execution ownership, restore verified bound
state, and implement both callbacks with confirmed remote readback. This adapter
is not a lease, a cloud launcher or a promise of exits during a core-store outage.
Archive-only failure blocks entries; critical-state failure stops broker writes.
In particular an outage after cancellation can still leave protection unavailable.
Risk evidence is a timestamped observation, not an atomic broker snapshot. After
the final evidence commit, a bounded fresh risk/quote check can reject an entry;
its last volatile observations are not claimed as remotely archived evidence.
"""

from copy import deepcopy
from datetime import timedelta
import hashlib
import math
import re

from .broker import BrokerError, OrderAmbiguous, OrderRejected
from .cloud_checkpoint import CheckpointReceipt, LoadedCheckpoint, _canonical, _check_state
from .data import NY, timestamp
from .engine import Engine, TERMINAL, flatten
from .risk import drawdown, validated_quote


class CloudDurabilityError(RuntimeError):
    pass


def _nonnegative(value):
    try:
        result = float(value)
        if isinstance(value, bool) or not math.isfinite(result) or result < 0:
            raise ValueError
        return result
    except (TypeError, ValueError, OverflowError):
        raise ValueError("Invalid finite nonnegative broker quantity or value.") from None


def _order_numbers(order):
    quantity = _nonnegative(order["qty"])
    filled = _nonnegative(order.get("filled_qty") or 0)
    limit = _nonnegative(order.get("limit_price") or 0)
    if quantity <= 0 or filled > quantity or (order["side"] == "buy" and limit <= 0):
        raise ValueError("Invalid current order amounts.")
    return quantity, filled, limit


class CloudEngine(Engine):
    """Keep the stock strategies, with explicit remote barriers for broker writes.

    checkpoint(state_copy) -> CheckpointReceipt or LoadedCheckpoint.
    evidence_barrier(state_copy) -> BundleReceipt (a CheckpointReceipt subclass),
    including every archive record written before the call, exact input/sample
    evidence and the order intent. Both callbacks MUST share one expected-HEAD
    coordinator. A receipt is trusted only after that callback's remote readback.
    No callback may perform a broker operation. A receipt is not a broker retry.
    """

    def __init__(self, broker, config, state, directory, *, checkpoint,
                 evidence_barrier, audit=None, run_id=None):
        _check_state(state)
        if not callable(checkpoint) or not callable(evidence_barrier):
            raise ValueError("Cloud persistence callbacks are required.")
        super().__init__(broker, config, state, directory, audit=audit, run_id=run_id)
        self._checkpoint = checkpoint
        self._evidence_barrier = evidence_barrier
        self._last_receipt = None
        self._durability_failed = False
        self._latched_halt = state["halted"]
        self._peak_floor = state["equity_peak"]

    def _confirmed(self, result, state):
        if isinstance(result, LoadedCheckpoint):
            if _canonical(result.state) != _canonical(state):
                raise CloudDurabilityError("Checkpoint readback differs from the proposed state.")
            result = result.receipt
        if (not isinstance(result, CheckpointReceipt)
                or not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", result.commit)
                or result.state_sha256 != hashlib.sha256(_canonical(state)).hexdigest()):
            raise CloudDurabilityError("A matching confirmed checkpoint receipt is required.")
        if self._last_receipt is not None:
            unchanged = (result.commit, result.previous_commit, result.state_sha256) == (
                self._last_receipt.commit, self._last_receipt.previous_commit, self._last_receipt.state_sha256)
            if not unchanged and result.previous_commit != self._last_receipt.commit:
                raise CloudDurabilityError("Checkpoint receipt does not continue this execution's head.")
        self._last_receipt = result
        return result

    def save(self):
        if self._durability_failed:
            raise CloudDurabilityError("Core persistence is unavailable; reload and reconcile before execution.")
        # Base Engine may add a different halt reason on a later observation. The
        # first latch remains authoritative; it is never cleared or replaced here.
        self._latched_halt = self._latched_halt or self.state["halted"]
        self.state["halted"] = self._latched_halt
        self._peak_floor = max(self._peak_floor, self.state["equity_peak"])
        self.state["equity_peak"] = self._peak_floor
        try:
            super().save()
            proposed = deepcopy(self.state)
            _check_state(proposed)
            self._confirmed(self._checkpoint(proposed), proposed)
        except Exception:
            self._durability_failed = True
            raise CloudDurabilityError("Core checkpoint was not confirmed; no further broker writes are allowed.") from None

    def _archive_barrier(self):
        if not self.audit_available:
            return False
        try:
            proposed = deepcopy(self.state)
            result = self._evidence_barrier(proposed)
            receipt = result.receipt if isinstance(result, LoadedCheckpoint) else result
            if (not isinstance(receipt, CheckpointReceipt)
                    or any(not isinstance(getattr(receipt, name, None), str)
                           or re.fullmatch(r"[0-9a-f]{64}", getattr(receipt, name)) is None
                           for name in ("archive_manifest_sha256", "archive_snapshot_sha256", "archive_index_sha256"))
                    or type(getattr(receipt, "generation", None)) is not int or receipt.generation < 0):
                raise CloudDurabilityError("Confirmed archive bundle evidence is required.")
            self._confirmed(result, proposed)
            return True
        except Exception:
            self.audit_available = False
            self.alerts.append("Remote evidence was not confirmed; new entries blocked.")
            return False

    def _not_sent(self, intent, reason):
        intent.update(status="not_submitted", reason=reason)
        self.save()
        return False

    def _entry_clock_guard(self, sleeve, context):
        clock = self.broker.clock()
        now = timestamp(clock["timestamp"]).astimezone(NY)
        if (not clock["is_open"] or (now.hour, now.minute) < (9, 45)
                or now >= timestamp(clock["next_close"]) - timedelta(minutes=45)
                or (sleeve == "intraday" and (now.hour, now.minute) >= (14, 30))):
            raise ValueError("Entry window ended during persistence.")
        validated_quote(context["quote"], now, self.config)
        if sleeve == "intraday" and not 0 <= (now - timestamp(context["signal"]["signal_time"])).total_seconds() < 900:
            raise ValueError("Signal expired during persistence.")
        return clock, now

    def _fresh_buy_guard(self, payload, sleeve, context, *, record=True):
        clock, now = self._entry_clock_guard(sleeve, context)
        account = self.broker.account()
        if account.get("id") != self.state["account_id"]:
            raise ValueError("Bound account changed.")
        equity, cash = _nonnegative(account["equity"]), _nonnegative(account["cash"])
        previous_peak = self.state["equity_peak"]
        self.state["equity_peak"] = max(previous_peak, equity)
        if drawdown(equity, self.state["equity_peak"]) >= self.config["max_drawdown"] - 1e-9:
            self.state["halted"] = self.state["halted"] or "10% account drawdown reached; cancel entries and flatten owned positions."
        if previous_peak != self.state["equity_peak"] or self.state["halted"]:
            self.save()
        if (self.state["halted"] or self.state["paused"] or self.alerts
                or account.get("status") != "ACTIVE" or account.get("trading_blocked") or account.get("account_blocked")):
            raise ValueError("Current account or risk state blocks entry.")
        positions = self.broker.positions()
        orders = [order for order in flatten(self.broker.orders()) if order["status"] not in TERMINAL]
        for position in positions:
            for name in ("qty", "market_value", "cost_basis"):
                _nonnegative(position.get(name, 0))
        for order in orders:
            _order_numbers(order)
        known = set(self.state.get("known_order_ids", [])) | {intent.get("order_id") for intent in self.state["intents"].values()}
        if (any(position["symbol"] not in self.state["owners"] for position in positions)
                or any(order["id"] not in known for order in orders)):
            self.alerts.append("Unattributed activity found after persistence; manual reconciliation required.")
            raise ValueError("Unattributed activity blocks entry.")
        held = {position["symbol"] for position in positions}
        active = {order["symbol"] for order in orders}
        if payload["symbol"] in held | active or any(float(position["qty"]) < 0 for position in positions):
            raise ValueError("Current inventory blocks entry.")
        reserve = sum(max(0., float(order["qty"]) - float(order.get("filled_qty") or 0)) * float(order.get("limit_price") or 0)
                      for order in orders if order["side"] == "buy")
        gross = sum(max(float(position["market_value"]), float(position.get("cost_basis") or 0)) for position in positions)
        available = min(cash - reserve, self.config["capital"] - gross - reserve) - self.config["capital"] * self.config["cash_reserve_fraction"]
        budget = self.config[sleeve]["allocation"]
        belongs = lambda symbol: self.state["owners"].get(symbol, {}).get("sleeve") == sleeve
        exposure = sum(max(float(position["market_value"]), float(position.get("cost_basis") or 0)) for position in positions if belongs(position["symbol"]))
        pending = sum(max(0., float(order["qty"]) - float(order.get("filled_qty") or 0)) * float(order.get("limit_price") or 0)
                      for order in orders if order["side"] == "buy" and belongs(order["symbol"]))
        slots = min(self.config[sleeve]["max_positions"], int(self.state["strategies"].get(sleeve, {}).get("params", {}).get("top_n", self.config[sleeve]["max_positions"])))
        cost = int(payload["qty"]) * float(payload["limit_price"])
        if (slots <= 0 or not all(math.isfinite(value) for value in (reserve, gross, available, exposure, pending))
                or len({symbol for symbol in held | active if belongs(symbol)}) >= slots
                or not math.isfinite(cost) or cost <= 0 or cost > min(available, budget - exposure - pending, budget / slots) + 1e-8):
            raise ValueError("Persisted entry no longer fits current cash, slots or sleeve budget.")
        evidence = {"client_order_id": payload["client_order_id"],
                         "account": {key: account.get(key) for key in
                         ("status", "cash", "equity", "trading_blocked", "account_blocked")},
                         "positions": positions, "orders": orders, "available": available,
                         "sleeve_available": budget - exposure - pending}
        if record:
            self.audit_event("cloud_pre_submit_guard", {**evidence, "clock": clock})
        return now

    def submit(self, payload, sleeve, now):
        # Sells must use the exit planner, which persists before canceling stops.
        if payload["side"] != "buy":
            raise ValueError("Cloud exits must use the durable exit planner.")
        cid = payload["client_order_id"]
        if cid in self.state["intents"] or not self.audit_available:
            return False
        context = deepcopy(getattr(self, "_entry_context", None))
        if not isinstance(context, dict) or "quote" not in context:
            raise ValueError("Pre-order decision and quote evidence are required.")
        intent = {"payload": deepcopy(payload), "sleeve": sleeve, "status": "submitting", "created": now.isoformat()}
        self.state["intents"][cid] = intent
        self.state["owners"][payload["symbol"]] = {"sleeve": sleeve, "entry_cid": cid, "entry_day": now.date().isoformat()}
        self.save()
        self.audit_event("order_intent", {"client_order_id": cid, "sleeve": sleeve, "payload": payload,
                         "strategy": self.state["strategies"].get(sleeve), "context": context})
        if not self._archive_barrier():
            return self._not_sent(intent, "archive_failure_before_submit")
        try:
            guarded_at = self._fresh_buy_guard(payload, sleeve, context)
            if not self._archive_barrier():
                return self._not_sent(intent, "archive_failure_before_submit")
            # Re-read once after the final network barrier and reapply the same
            # hard limits. Ordinary price/P&L changes need not match the archived
            # snapshot byte-for-byte. Never resize or rewrite the durable order.
            self._fresh_buy_guard(payload, sleeve, context, record=False)
            _, final_now = self._entry_clock_guard(sleeve, context)
            if not 0 <= (final_now - guarded_at).total_seconds() <= self.config["max_quote_age_seconds"]:
                raise ValueError("Risk snapshot expired during persistence.")
        except (ValueError, KeyError, TypeError):
            return self._not_sent(intent, "fresh_pre_submit_check_failed")
        return self._send(intent, now)

    def _send(self, intent, now):
        if self._durability_failed or intent["status"] != "submitting":
            raise CloudDurabilityError("No confirmed dispatch intent is available.")
        payload, cid = intent["payload"], intent["payload"]["client_order_id"]
        try:
            order = self.broker.submit_order(deepcopy(payload))
        except OrderRejected as exc:
            self.audit_event("order_rejected", {"client_order_id": cid, "status": exc.status})
            intent["status"] = "rejected"
            self.alerts.append("Paper order was rejected; reconcile before a new exit attempt.")
            self.save()
            return False
        except (OrderAmbiguous, BrokerError):
            intent["status"] = "uncertain"
            self.audit_event("order_uncertain", {"client_order_id": cid, "next_action": "reconcile_without_resubmitting"})
            self.save()
            raise CloudDurabilityError("Broker outcome is uncertain; reconcile the original client order ID.") from None
        self.audit_event("order_response", {"client_order_id": cid, "sleeve": intent["sleeve"], "broker_order": order})
        intent.update(order_id=order["id"], status=order["status"])
        known = self.state.setdefault("known_order_ids", [])
        known.extend(row["id"] for row in flatten([order]) if row["id"] not in known)
        self.record({"at": now.isoformat(), "event": "order_submitted", "sleeve": intent["sleeve"],
                     "symbol": payload["symbol"], "side": payload["side"], "qty": payload["qty"], "order_id": order["id"]})
        return True

    def _exit_plan(self, symbol, owner, quantity, orders, now, reason):
        previous = [intent for intent in self.state["intents"].values() if intent.get("exit_for") == owner["entry_cid"]]
        cid = "zoh-exit-" + hashlib.sha256(owner["entry_cid"].encode()).hexdigest()[:10] + "-" + str(len(previous) + 1)
        intent = {"payload": {"symbol": symbol, "qty": str(quantity), "side": "sell", "type": "market",
                              "time_in_force": "day", "position_intent": "sell_to_close", "client_order_id": cid},
                  "sleeve": owner["sleeve"], "created": now.isoformat(), "status": "exit_prepared",
                  "exit_for": owner["entry_cid"], "reason": reason, "cancel_order_ids": [order["id"] for order in orders]}
        self.state["intents"][cid] = intent
        self.audit_event("cloud_exit_prepared", deepcopy(intent))
        self.save()
        return intent

    def exit_position(self, symbol, owner, now, reason):
        owner["exit_reason"] = owner.get("exit_reason") or reason
        pending = [intent for intent in self.state["intents"].values()
                   if intent.get("exit_for") == owner["entry_cid"] and intent["status"] not in TERMINAL]
        if len(pending) > 1 or (pending and pending[0]["status"] != "exit_prepared"):
            return False  # submitting/uncertain + a broker 404 never authorizes another POST.
        orders = [order for order in flatten(self.broker.orders()) if order["symbol"] == symbol and order["status"] not in TERMINAL]
        known = set(self.state.get("known_order_ids", [])) | {intent.get("order_id") for intent in self.state["intents"].values()}
        if any(order["id"] not in known for order in orders):
            self.alerts.append("Untracked order on owned symbol; manual reconciliation required.")
            return False
        existing_exits = {intent.get("order_id") for intent in self.state["intents"].values()
                          if intent["payload"]["side"] == "sell"}
        if any(order["id"] in existing_exits for order in orders):
            self.alerts.append("An existing exit is still open; reconcile before another exit attempt.")
            return False
        positions = {position["symbol"]: position for position in self.broker.positions()}
        try:
            quantity = _nonnegative(positions.get(symbol, {}).get("qty", 0))
            for order in orders:
                _order_numbers(order)
            potential = sum(_order_numbers(order)[0] - _order_numbers(order)[1] for order in orders if order["side"] == "buy")
            if not quantity.is_integer() or not math.isfinite(potential) or not potential.is_integer():
                raise ValueError
        except (ValueError, TypeError, KeyError):
            self.alerts.append("Unexpected non-integer or short holding; exit blocked.")
            return False
        # When an entry has not filled, reserve its possible whole-share outcome.
        # This prepared payload is never sent unless the post-cancel holding equals it.
        planned_quantity = int(quantity if quantity else potential)
        if not pending and planned_quantity == 0:
            if orders:
                self.alerts.append("Orders without owned shares need reconciliation before cancellation.")
                return False
            return True
        intent = pending[0] if pending else self._exit_plan(symbol, owner, planned_quantity, orders, now, reason)
        intent["cancel_order_ids"] = sorted(set(intent["cancel_order_ids"]) | {order["id"] for order in orders})
        self.save()  # Confirm the exact complete plan before canceling any protection.
        if not self.broker.clock()["is_open"]:
            return False
        for order in orders:
            try:
                self.audit_event("cancel_requested", {"order_id": order["id"], "symbol": symbol, "reason": reason})
                self.broker.cancel_order(order["id"])
                self.audit_event("cancel_acknowledged", {"order_id": order["id"], "terminal_status_confirmed": False})
            except BrokerError:
                self.alerts.append("Cancellation not confirmed; exit deferred.")
                return False
        remaining = [order for order in flatten(self.broker.orders()) if order["symbol"] == symbol and order["status"] not in TERMINAL]
        if remaining:
            self.alerts.append("Waiting for cancel confirmation.")
            return False
        self.audit_event("cancel_confirmed", {"symbol": symbol, "open_orders_remaining": 0})
        positions = {position["symbol"]: position for position in self.broker.positions()}
        quantity = float(positions.get(symbol, {}).get("qty", 0))
        if quantity == 0:
            self._not_sent(intent, "position_absent_after_cancel")
            return True
        if not math.isfinite(quantity) or quantity <= 0 or not quantity.is_integer():
            self.alerts.append("Unexpected post-cancel quantity; exit blocked.")
            return False
        if quantity != int(intent["payload"]["qty"]):
            self._not_sent(intent, "position_changed_before_dispatch")
            intent = self._exit_plan(symbol, owner, int(quantity), [], now, reason)
        intent["status"] = "submitting"
        self.save()  # Never dispatch an exit_prepared plan directly, even on restart.
        final_positions = {position["symbol"]: position for position in self.broker.positions()}
        final_orders = [order for order in flatten(self.broker.orders()) if order["symbol"] == symbol and order["status"] not in TERMINAL]
        if (not self.broker.clock()["is_open"] or final_orders
                or float(final_positions.get(symbol, {}).get("qty", 0)) != int(intent["payload"]["qty"])):
            return self._not_sent(intent, "exit_pre_submit_state_changed")
        return self._send(intent, now)
