"""Small Alpaca PAPER-only client; secrets and provider errors never enter logs.

Credentials come from the existing current-user Windows DPAPI store. Callers
must reconcile ambiguous writes by client_order_id before taking another action.
The execution engine is responsible for market hours, allocation, and ensuring
sell quantities close existing long positions.
"""
from __future__ import annotations

import base64
import datetime as dt
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

PAPER_HOST = "https://paper-api.alpaca.markets"
DATA_HOST = "https://data.alpaca.markets"
_SYMBOL = re.compile(r"^[A-Z][A-Z0-9.\-]{0,14}$")
_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_CID = re.compile(r"^zoh-[A-Za-z0-9_-]{1,44}$")
_ACTIVITY_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,256}$")


class BrokerError(RuntimeError):
    """Sanitized error. status is an HTTP integer, or None for no response."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class OrderRejected(BrokerError):
    """The server definitively rejected this mutation (HTTP 4xx)."""


class OrderAmbiguous(BrokerError):
    """Write outcome unknown: reconcile with broker, never blindly retry."""


class ResponseIncomplete(BrokerError):
    """Completeness cannot be established. Do not trade on this snapshot."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def load_credentials() -> dict[str, str]:
    """Decrypt only within a hidden PowerShell child and capture its stdout."""
    if os.name != 'nt' and os.environ.get('ZOH_PRIVATE_RUNTIME'):
        # Deployment mounts a private secret file; never migrate Windows DPAPI.
        from pathlib import Path
        import stat
        try:
            path = Path(os.environ['ZOH_PAPER_SECRET_FILE'])
            info = path.lstat()
            if (not path.is_absolute() or not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid() or info.st_mode & 0o077):
                raise ValueError()
            value = json.loads(path.read_text(encoding='utf-8'))
            if (set(value) != {'key', 'secret'} or any(
                    not isinstance(value[k], str) or not value[k].strip()
                    for k in ('key', 'secret'))):
                raise ValueError()
            return value
        except Exception:
            raise BrokerError('Private PAPER secret mount is unavailable or invalid.') from None
    if os.name != "nt" or not os.environ.get("LOCALAPPDATA"):
        raise BrokerError("Windows current-user paper credentials are required.")
    script = r"""
$ErrorActionPreference = 'Stop'
try {
    $path = Join-Path $env:LOCALAPPDATA 'ZeroOrHero\alpaca-paper.credentials.xml'
    $protected = Import-Clixml -LiteralPath $path
    $temporary = [pscredential]::new('local', $protected)
    $payload = $temporary.GetNetworkCredential().Password
    [Console]::Out.Write($payload)
} catch { exit 3 }
finally { $payload = $null; $temporary = $null; if ($protected) { $protected.Dispose() } }
"""
    encoded = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive",
             "-EncodedCommand", encoded],
            capture_output=True, text=True, timeout=20, check=False,
            creationflags=0x08000000,
        )
        if result.returncode != 0:
            raise ValueError()
        value = json.loads(result.stdout)
        if (not isinstance(value, dict) or
                any(not isinstance(value.get(k), str) or not value[k].strip()
                    for k in ("key", "secret"))):
            raise ValueError()
        return {"key": value["key"], "secret": value["secret"]}
    except Exception:
        raise BrokerError("Cannot read paper credentials; run local paper setup as the same Windows user.") from None


def _symbol(value: str) -> str:
    if not isinstance(value, str) or not _SYMBOL.fullmatch(value):
        raise BrokerError("Invalid US equity symbol.")
    return value


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise BrokerError("Invalid order identifier.")
    return value


def _date(value) -> str:
    try:
        text = value.isoformat() if isinstance(value, dt.date) else str(value)
        return dt.date.fromisoformat(text).isoformat()
    except (ValueError, TypeError):
        raise BrokerError("Expected a calendar date in YYYY-MM-DD format.") from None


def _time_bound(value) -> str:
    if isinstance(value, (dt.datetime, dt.date)):
        value = value.isoformat()
    if not isinstance(value, str) or not value or len(value) > 64:
        raise BrokerError("Invalid historical data date boundary.")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if "T" in value and parsed.tzinfo is None:
            raise ValueError()
    except ValueError:
        raise BrokerError("Historical timestamps must include a timezone, or use YYYY-MM-DD.") from None
    return value


def _price(value) -> str:
    try:
        if isinstance(value, bool):
            raise ValueError()
        amount = Decimal(str(value))
        if not amount.is_finite() or amount <= 0 or amount > Decimal("10000000"):
            raise ValueError()
        amount = amount.quantize(Decimal("0.01") if amount >= 1 else Decimal("0.0001"), rounding=ROUND_HALF_UP)
        if amount <= 0:
            raise ValueError()
        return format(amount, "f")
    except (ValueError, InvalidOperation, TypeError):
        raise BrokerError("Order prices must be finite and positive.") from None


class PaperBroker:
    """No live URL or endpoint configuration exists.

    Optional transport(request, timeout) -> (status, bytes) is for isolated
    tests. GET has at most three attempts for 429/5xx; writes are attempted once.
    """

    def __init__(self, *, credential_loader=None, transport=None, timeout=20,
                 sleeper=time.sleep):
        try:
            creds = (credential_loader or load_credentials)()
            if (not isinstance(creds, dict) or
                    any(not isinstance(creds.get(k), str) or not creds[k].strip()
                        or "\r" in creds[k] or "\n" in creds[k]
                        for k in ("key", "secret"))):
                raise ValueError()
            self._key, self._secret = creds["key"], creds["secret"]
        except Exception:
            raise BrokerError("Paper credentials unavailable or invalid.") from None
        self._timeout = timeout
        self._sleeper = sleeper
        self._opener = urllib.request.build_opener(_NoRedirect())
        self._transport = transport or self._http

    def _http(self, request, timeout):
        try:
            with self._opener.open(request, timeout=timeout) as response:
                # Defense in depth if transport behavior ever changes.
                if response.geturl() != request.full_url:
                    raise BrokerError("HTTP redirect refused.")
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            exc.close()  # Never read, retain, or echo a provider error body.
            return status, b""

    def _request(self, method, path, *, data=False, params=None, payload=None):
        if not isinstance(path, str) or not path.startswith("/v2/") or "?" in path or "#" in path:
            raise BrokerError("Invalid fixed-endpoint API path.")
        if method not in {"GET", "POST", "DELETE"} or (data and method != "GET"):
            raise BrokerError("API operation is not permitted.")
        url = (DATA_HOST if data else PAPER_HOST) + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        body = None if payload is None else json.dumps(payload, allow_nan=False).encode("utf-8")
        attempts = 3 if method == "GET" else 1
        for attempt in range(attempts):
            request = urllib.request.Request(url, data=body, method=method, headers={
                "APCA-API-KEY-ID": self._key,
                "APCA-API-SECRET-KEY": self._secret,
                "Accept": "application/json", "Content-Type": "application/json",
                "User-Agent": "ZeroOrHero-Paper/1.0",
            })
            try:
                status, raw = self._transport(request, self._timeout)
                if not isinstance(status, int):
                    raise ValueError()
            except Exception:
                error = OrderAmbiguous if method != "GET" else BrokerError
                raise error("Paper API connection failed; mutation outcome requires reconciliation." if method != "GET"
                            else "Paper API connection failed.") from None
            if (status == 429 or 500 <= status < 600) and attempt + 1 < attempts:
                self._sleeper(min(2 ** attempt, 4))
                continue
            if not 200 <= status < 300:
                if method != "GET":
                    error = OrderRejected if 400 <= status < 500 else OrderAmbiguous
                else:
                    error = BrokerError
                raise error(f"Paper API {method} failed (HTTP {status}).", status=status)
            if status == 204:
                return None
            try:
                return json.loads(raw)
            except (ValueError, TypeError, UnicodeDecodeError):
                error = OrderAmbiguous if method != "GET" else BrokerError
                raise error("Paper API returned an invalid response.", status=status) from None
        raise BrokerError("Paper API unavailable.")

    @staticmethod
    def _object(value):
        if not isinstance(value, dict):
            raise BrokerError("Paper API response must be an object.")
        return value

    @staticmethod
    def _list(value):
        if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
            raise BrokerError("Paper API response must be a list of objects.")
        return value

    def account(self) -> dict:
        return self._object(self._request("GET", "/v2/account"))

    def clock(self) -> dict:
        return self._object(self._request("GET", "/v2/clock"))

    def calendar(self, start, end) -> list:
        return self._list(self._request("GET", "/v2/calendar", params={"start": _date(start), "end": _date(end)}))

    def positions(self) -> list:
        return self._list(self._request("GET", "/v2/positions"))

    def orders(self, status="open") -> list:
        if status not in {"open", "closed", "all"}:
            raise BrokerError("Invalid order status filter.")
        rows = self._list(self._request("GET", "/v2/orders", params={
            "status": status, "nested": "true", "limit": 500, "direction": "asc",
        }))
        if len(rows) >= 500:
            raise ResponseIncomplete("Order response may be truncated; trading must wait for a complete snapshot.")
        return rows

    def fill_activities(self, after=None, until=None) -> list:
        """Return complete broker execution activities, including partial fills.

        Boundaries filter activity creation time and are exclusive. Across polls,
        callers should overlap the prior window and deduplicate by activity ID;
        transaction_time alone is not a safe cursor. Within a request, use the
        last activity ID so executions sharing a timestamp cannot be skipped.
        Original fields and decimal strings are preserved, not aggregated into
        order averages. This endpoint does not provide fees or a realized P&L.
        """
        params = {"direction": "asc", "page_size": 100}
        for name, value in (("after", after), ("until", until)):
            if value is not None:
                boundary = _time_bound(value)
                # Date-only bounds are supported; intraday bounds need a zone.
                if len(boundary) != 10 and dt.datetime.fromisoformat(boundary.replace("Z", "+00:00")).tzinfo is None:
                    raise BrokerError("Activity timestamps must include a timezone, or use YYYY-MM-DD.")
                params[name] = boundary
        result, seen = [], set()
        for _ in range(10000):
            rows = self._list(self._request("GET", "/v2/account/activities/FILL", params=params))
            if not rows:
                return result
            if len(rows) > 100:
                raise ResponseIncomplete("Activity response exceeded the requested page size.")
            for row in rows:
                activity_id = row.get("id")
                if (not isinstance(activity_id, str) or not _ACTIVITY_ID.fullmatch(activity_id)
                        or row.get("activity_type") != "FILL"):
                    raise ResponseIncomplete("Fill activity response has an invalid type or identifier.")
                timestamp = row.get("transaction_time")
                try:
                    if not isinstance(timestamp, str) or len(timestamp) > 64 or "T" not in timestamp:
                        raise ValueError()
                    parsed = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    if parsed.tzinfo is None:
                        raise ValueError()
                except ValueError:
                    raise ResponseIncomplete("Fill activity response has an invalid execution timestamp.") from None
                if activity_id in seen:
                    raise ResponseIncomplete("Fill activity pagination repeated an activity; completeness is uncertain.")
                seen.add(activity_id)
            result.extend(rows)
            params["page_token"] = rows[-1]["id"]
        raise ResponseIncomplete("Fill activity pagination limit reached.")

    def order_by_client_id(self, cid: str) -> dict | None:
        _identifier(cid)
        try:
            return self._object(self._request("GET", "/v2/orders:by_client_order_id", params={"client_order_id": cid}))
        except BrokerError as exc:
            if exc.status == 404:
                return None
            raise

    def asset(self, symbol: str) -> dict:
        return self._object(self._request("GET", "/v2/assets/" + _symbol(symbol)))

    def daily_bars(self, symbols, start, end) -> dict[str, list]:
        return self._bars(symbols, start, end, "1Day")

    def intraday_bars(self, symbols, start, end, timeframe="15Min") -> dict[str, list]:
        if timeframe not in {"15Min", "5Min", "1Min"}:
            raise BrokerError("Intraday timeframe must be 15Min, 5Min, or 1Min.")
        return self._bars(symbols, start, end, timeframe)

    def _bars(self, symbols, start, end, timeframe):
        if isinstance(symbols, str):
            symbols = [symbols]
        symbols = list(dict.fromkeys(_symbol(s) for s in symbols))
        if not symbols:
            return {}
        start, end = _time_bound(start), _time_bound(end)
        result = {symbol: [] for symbol in symbols}
        for offset in range(0, len(symbols), 100):
            batch = symbols[offset:offset + 100]
            params = {"symbols": ",".join(batch), "timeframe": timeframe,
                      "start": start, "end": end, "adjustment": "split",
                      "feed": "iex", "sort": "asc", "limit": 10000}
            seen = set()
            for _ in range(10000):
                page = self._object(self._request("GET", "/v2/stocks/bars", data=True, params=params))
                bars = page.get("bars")
                if not isinstance(bars, dict):
                    raise BrokerError("Historical bar response is invalid.")
                if any(symbol not in batch for symbol in bars):
                    raise BrokerError("Historical bar response contains an unexpected symbol.")
                for symbol, rows in bars.items():
                    result[symbol].extend(self._list(rows))
                token = page.get("next_page_token")
                if token is None or token == "":
                    break
                if not isinstance(token, str) or token in seen:
                    raise ResponseIncomplete("Historical pagination did not advance.")
                seen.add(token)
                params["page_token"] = token
            else:
                raise ResponseIncomplete("Historical data pagination limit reached.")
        return result

    def latest_quote(self, symbol: str) -> dict:
        value = self._object(self._request("GET", "/v2/stocks/" + _symbol(symbol) + "/quotes/latest",
                                         data=True, params={"feed": "iex"}))
        return self._object(value.get("quote"))

    def submit_order(self, payload: dict) -> dict:
        """One attempt only. Engine must enforce long-only inventory and risk."""
        if not isinstance(payload, dict):
            raise BrokerError("Order payload must be an object.")
        common = {"symbol", "qty", "side", "type", "time_in_force", "client_order_id"}
        allowed = common | {"order_class", "stop_loss", "limit_price", "extended_hours", "position_intent"}
        if not common.issubset(payload) or set(payload) - allowed:
            raise BrokerError("Order payload has missing or unsupported fields.")
        result = dict(payload)
        result["symbol"] = _symbol(payload["symbol"])
        qty = payload["qty"]
        if isinstance(qty, bool) or not isinstance(qty, (int, str)) or not re.fullmatch(r"[1-9][0-9]*", str(qty)):
            raise BrokerError("Order quantity must be a positive whole number.")
        result["qty"] = str(qty)
        if not isinstance(payload["client_order_id"], str) or not _CID.fullmatch(payload["client_order_id"]):
            raise BrokerError("Client order identifier must use the zoh- prefix and be at most 48 characters.")
        if payload.get("extended_hours", False) is not False:
            raise BrokerError("Extended-hours orders are disabled.")
        if payload["side"] == "buy":
            if (payload["type"] != "limit" or payload.get("order_class") != "oto"
                    or payload["time_in_force"] not in {"day", "gtc"}
                    or payload.get("position_intent", "buy_to_open") != "buy_to_open"):
                raise BrokerError("Entries must be long limit OTO orders with a protective stop.")
            stop = payload.get("stop_loss")
            if not isinstance(stop, dict) or set(stop) != {"stop_price"}:
                raise BrokerError("Entries require a stop-market protective order.")
            result["limit_price"] = _price(payload.get("limit_price"))
            result["stop_loss"] = {"stop_price": _price(stop["stop_price"])}
            if Decimal(result["limit_price"]) - Decimal(result["stop_loss"]["stop_price"]) < Decimal("0.01"):
                raise BrokerError("Protective stop must be at least one cent below the entry limit.")
        elif payload["side"] == "sell":
            if (payload["type"] != "market" or payload["time_in_force"] != "day"
                    or payload.get("order_class", "simple") != "simple"
                    or "stop_loss" in payload or "limit_price" in payload
                    or payload.get("position_intent", "sell_to_close") != "sell_to_close"):
                raise BrokerError("Exits must be long-position day market orders.")
        else:
            raise BrokerError("Unsupported order side.")
        value = self._request("POST", "/v2/orders", payload=result)
        if not isinstance(value, dict) or not value.get("id"):
            raise OrderAmbiguous("Order response lacks an identifier; reconcile by client order identifier.")
        return value

    def cancel_order(self, order_id: str) -> None:
        self._request("DELETE", "/v2/orders/" + _identifier(order_id))

    def get_order(self, order_id: str) -> dict:
        return self._object(self._request("GET", "/v2/orders/" + _identifier(order_id), params={"nested": "true"}))
