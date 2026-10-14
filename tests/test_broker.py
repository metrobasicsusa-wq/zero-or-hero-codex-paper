import copy
import io
import json
import unittest
import urllib.error
import urllib.parse
import urllib.request
from unittest.mock import patch

from zerotrader.broker import (
    BrokerError, OrderAmbiguous, OrderRejected, PaperBroker,
    ResponseIncomplete, _NoRedirect,
)


class FakeTransport:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append(request)
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        status, body = value
        return status, body if isinstance(body, bytes) else json.dumps(body).encode()


def broker(transport):
    return PaperBroker(credential_loader=lambda: {"key": "private-key", "secret": "private-secret"},
                       transport=transport, sleeper=lambda seconds: None)


def buy():
    return {"symbol": "AAPL", "qty": 3, "side": "buy", "type": "limit",
            "time_in_force": "gtc", "order_class": "oto", "limit_price": "201.20",
            "stop_loss": {"stop_price": "195.00"}, "client_order_id": "zoh-entry-test"}


def sell():
    return {"symbol": "AAPL", "qty": "3", "side": "sell", "type": "market",
            "time_in_force": "day", "client_order_id": "zoh-exit-test"}


def fill_activity(number, **changes):
    value = {"id": f"20260930143000000::activity-{number}", "activity_type": "FILL",
             "transaction_time": "2026-09-30T14:30:00.000000001Z", "type": "partial_fill",
             "price": "123.456789", "qty": "0.123456789", "side": "buy", "symbol": "AAPL",
             "order_id": "order-123", "cum_qty": "0.123456789", "leaves_qty": "0.876543211"}
    value.update(changes)
    return value


class BrokerTests(unittest.TestCase):
    def test_fixed_paper_and_iex_data_endpoints(self):
        transport = FakeTransport((200, {"status": "ACTIVE"}), (200, {"quote": {"ap": 10}}))
        api = broker(transport)
        api.account()
        self.assertEqual(api.latest_quote("AAPL"), {"ap": 10})
        self.assertEqual(transport.requests[0].full_url, "https://paper-api.alpaca.markets/v2/account")
        self.assertEqual(transport.requests[1].full_url, "https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest?feed=iex")
        with self.assertRaises(TypeError):
            PaperBroker(base_url="https://api.alpaca.markets")
        self.assertNotIn("private-secret", repr(api))

    def test_default_transport_has_redirect_handler_that_refuses_redirect(self):
        api = broker(FakeTransport())
        handlers = [h for h in api._opener.handlers if isinstance(h, urllib.request.HTTPRedirectHandler)]
        self.assertEqual(len(handlers), 1)
        self.assertIsInstance(handlers[0], _NoRedirect)
        request = urllib.request.Request("https://paper-api.alpaca.markets/v2/account")
        self.assertIsNone(handlers[0].redirect_request(request, None, 302, "redirect", {}, "https://api.alpaca.markets"))
        error = urllib.error.HTTPError(request.full_url, 302, "private-secret", {}, io.BytesIO(b"private-secret"))
        with patch.object(api._opener, "open", side_effect=error) as opener:
            self.assertEqual(api._http(request, 1), (302, b""))
        self.assertEqual(opener.call_count, 1)

    def test_redirect_response_is_never_followed(self):
        transport = FakeTransport((302, {"location": "https://api.alpaca.markets", "secret": "private-secret"}))
        with self.assertRaises(BrokerError) as caught:
            broker(transport).account()
        self.assertEqual(caught.exception.status, 302)
        self.assertNotIn("private-secret", str(caught.exception))
        self.assertEqual(len(transport.requests), 1)

    def test_daily_bars_paginate_across_symbols(self):
        transport = FakeTransport(
            (200, {"bars": {"AAPL": [{"t": "2026-01-01", "c": 1}]}, "next_page_token": "page2"}),
            (200, {"bars": {"AAPL": [{"t": "2026-01-02", "c": 2}], "MSFT": [{"t": "2026-01-01", "c": 3}]}, "next_page_token": None}),
        )
        values = broker(transport).daily_bars(["AAPL", "MSFT", "AAPL"], "2026-01-01", "2026-01-03")
        self.assertEqual([r["c"] for r in values["AAPL"]], [1, 2])
        self.assertEqual(values["MSFT"][0]["c"], 3)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(transport.requests[1].full_url).query)
        self.assertEqual(query["page_token"], ["page2"])
        self.assertEqual(query["timeframe"], ["1Day"])
        self.assertEqual(query["feed"], ["iex"])
        self.assertEqual(query["adjustment"], ["split"])
        self.assertEqual(query["sort"], ["asc"])

    def test_nonadvancing_or_malformed_pagination_fails_closed(self):
        for token in ("same", 7):
            with self.subTest(token=token):
                transport = FakeTransport(
                    (200, {"bars": {}, "next_page_token": "same"}),
                    (200, {"bars": {}, "next_page_token": token}),
                )
                with self.assertRaises(ResponseIncomplete):
                    broker(transport).daily_bars(["AAPL"], "2026-01-01", "2026-01-03")

    def test_intraday_timeframes_and_timezone(self):
        for timeframe in ("1Min", "5Min", "15Min"):
            transport = FakeTransport((200, {"bars": {"AAPL": []}, "next_page_token": None}))
            self.assertEqual(broker(transport).intraday_bars(["AAPL"], "2026-01-01T14:30:00Z", "2026-01-02T21:00:00+00:00", timeframe), {"AAPL": []})
            self.assertIn("timeframe=" + timeframe, transport.requests[0].full_url)
        api = broker(FakeTransport())
        with self.assertRaises(BrokerError):
            api.intraday_bars(["AAPL"], "2026-01-01", "2026-01-02", "1Hour")
        with self.assertRaises(BrokerError):
            api.daily_bars(["AAPL"], "2026-01-01T10:00:00", "2026-01-02")

    def test_truncated_orders_stop_instead_of_hiding_positions(self):
        transport = FakeTransport((200, [{"id": str(i)} for i in range(500)]))
        with self.assertRaises(ResponseIncomplete):
            broker(transport).orders()
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(transport.requests[0].full_url).query)
        self.assertEqual(query["nested"], ["true"])
        self.assertEqual(query["limit"], ["500"])

    def test_fill_activities_page_by_id_without_skipping_shared_timestamps(self):
        first = [fill_activity(i) for i in range(100)]
        second = [fill_activity(100, type="fill", qty="0.876543211", cum_qty="1", leaves_qty="0")]
        transport = FakeTransport((200, first), (200, second), (200, []))
        values = broker(transport).fill_activities(after="2026-09-29", until="2026-10-01T00:00:00Z")
        self.assertEqual(values, first + second)
        self.assertEqual(values[0]["price"], "123.456789")
        self.assertEqual(values[-1]["type"], "fill")
        for index, request in enumerate(transport.requests):
            self.assertEqual(request.get_method(), "GET")
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.netloc, "paper-api.alpaca.markets")
            self.assertEqual(parsed.path, "/v2/account/activities/FILL")
            params = urllib.parse.parse_qs(parsed.query)
            self.assertEqual(params["direction"], ["asc"])
            self.assertEqual(params["page_size"], ["100"])
            self.assertEqual(params["after"], ["2026-09-29"])
            self.assertEqual(params["until"], ["2026-10-01T00:00:00Z"])
            if index == 0:
                self.assertNotIn("page_token", params)
            else:
                expected = first[-1]["id"] if index == 1 else second[-1]["id"]
                self.assertEqual(params["page_token"], [expected])

    def test_fill_activities_default_has_no_implicit_timestamp_filter(self):
        transport = FakeTransport((200, []))
        self.assertEqual(broker(transport).fill_activities(), [])
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(transport.requests[0].full_url).query)
        self.assertEqual(params, {"direction": ["asc"], "page_size": ["100"]})

    def test_fill_activities_repeated_ids_fail_even_on_short_pages(self):
        for responses in (
            [(200, [fill_activity(1)]), (200, [fill_activity(1)])],
            [(200, [fill_activity(1), fill_activity(1)])],
            [(200, [fill_activity(1)]), (200, [fill_activity(2), fill_activity(1)])],
        ):
            with self.subTest(responses=responses):
                with self.assertRaises(ResponseIncomplete):
                    broker(FakeTransport(*responses)).fill_activities()

    def test_fill_activities_bad_records_are_sanitized_and_not_partially_returned(self):
        changes = [
            {"id": None}, {"id": "private-secret\n"}, {"activity_type": "DIV"},
            {"transaction_time": "private-secret"}, {"transaction_time": "2026-09-30"},
            {"transaction_time": "2026-09-30T14:30:00"}, {"transaction_time": None},
        ]
        for change in changes:
            with self.subTest(change=change):
                transport = FakeTransport((200, [fill_activity(1)]), (200, [fill_activity(2, **change)]))
                with self.assertRaises(ResponseIncomplete) as caught:
                    broker(transport).fill_activities()
                self.assertNotIn("private-secret", str(caught.exception))
                self.assertEqual(len(transport.requests), 2)
        with self.assertRaises(ResponseIncomplete):
            broker(FakeTransport((200, [fill_activity(i) for i in range(101)]))).fill_activities()

    def test_fill_activities_invalid_boundaries_do_not_reach_network(self):
        transport = FakeTransport()
        api = broker(transport)
        for value in ("2026-09-30T14:30:00", "2026-09-30 14:30:00", "private-secret", "", 123):
            with self.subTest(value=value), self.assertRaises(BrokerError):
                api.fill_activities(after=value)
        self.assertEqual(transport.requests, [])

    def test_fill_activities_later_page_failure_never_returns_partial_history(self):
        transport = FakeTransport((200, [fill_activity(1)]), (401, b"private-secret"))
        with self.assertRaises(BrokerError) as caught:
            broker(transport).fill_activities()
        self.assertEqual(caught.exception.status, 401)
        self.assertNotIn("private-secret", str(caught.exception))

    def test_get_transient_retries_are_bounded(self):
        transport = FakeTransport((429, b"private-secret"), (503, b"private-secret"), (200, {"status": "ACTIVE"}))
        self.assertEqual(broker(transport).account()["status"], "ACTIVE")
        self.assertEqual(len(transport.requests), 3)
        transport = FakeTransport(*[(503, b"private-secret")] * 4)
        with self.assertRaises(BrokerError) as caught:
            broker(transport).account()
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(len(transport.requests), 3)

    def test_client_id_not_found_distinct_from_other_failures(self):
        self.assertIsNone(broker(FakeTransport((404, b""))).order_by_client_id("zoh-unknown"))
        with self.assertRaises(BrokerError):
            broker(FakeTransport((401, b""))).order_by_client_id("zoh-unknown")

    def test_permitted_entry_and_exit_payloads(self):
        transport = FakeTransport((200, {"id": "entry"}), (200, {"id": "exit"}))
        api = broker(transport)
        api.submit_order(buy())
        api.submit_order(sell())
        entry = json.loads(transport.requests[0].data)
        self.assertEqual(entry["qty"], "3")
        self.assertEqual(entry["order_class"], "oto")
        self.assertEqual(entry["stop_loss"], {"stop_price": "195.00"})
        self.assertTrue(all(r.get_method() == "POST" and r.full_url == "https://paper-api.alpaca.markets/v2/orders" for r in transport.requests))

    def test_disallowed_order_shapes_do_not_reach_network(self):
        mutations = [
            {"type": "market"}, {"order_class": "simple"}, {"qty": 1.5},
            {"qty": True}, {"qty": "0"}, {"qty": "3.0"}, {"client_order_id": "other-bot"},
            {"extended_hours": True}, {"notional": 500}, {"position_intent": "buy_to_close"},
            {"stop_loss": {"stop_price": 195, "limit_price": 190}},
            {"stop_loss": {"stop_price": 202}}, {"limit_price": "NaN"},
            {"stop_loss": {"stop_price": "Infinity"}}, {"symbol": "BTC/USD"},
        ]
        transport = FakeTransport()
        api = broker(transport)
        for mutation in mutations:
            with self.subTest(mutation=mutation), self.assertRaises(BrokerError):
                payload = copy.deepcopy(buy())
                payload.update(mutation)
                api.submit_order(payload)
        for mutation in ({"type": "limit"}, {"position_intent": "sell_to_open"}, {"stop_loss": {"stop_price": 1}}, {"time_in_force": "gtc"}):
            with self.subTest(mutation=mutation), self.assertRaises(BrokerError):
                payload = sell()
                payload.update(mutation)
                api.submit_order(payload)
        self.assertEqual(transport.requests, [])

    def test_post_is_not_retried_and_errors_are_secret_safe(self):
        cases = [(422, OrderRejected), (429, OrderRejected), (500, OrderAmbiguous), (302, OrderAmbiguous)]
        for status, error in cases:
            with self.subTest(status=status):
                transport = FakeTransport((status, b"private-secret private-key"))
                with self.assertRaises(error) as caught:
                    broker(transport).submit_order(buy())
                self.assertEqual(caught.exception.status, status)
                self.assertNotIn("private-secret", str(caught.exception))
                self.assertNotIn("private-key", str(caught.exception))
                self.assertEqual(len(transport.requests), 1)
        transport = FakeTransport(urllib.error.URLError("private-secret private-key"))
        with self.assertRaises(OrderAmbiguous) as caught:
            broker(transport).submit_order(buy())
        self.assertNotIn("private-secret", str(caught.exception))
        self.assertTrue(caught.exception.__suppress_context__)
        self.assertEqual(len(transport.requests), 1)

    def test_success_status_with_invalid_order_response_is_ambiguous(self):
        for body in (b"not-json private-secret", {}, [], None):
            with self.subTest(body=body):
                transport = FakeTransport((200, body))
                with self.assertRaises(OrderAmbiguous):
                    broker(transport).submit_order(buy())
                self.assertEqual(len(transport.requests), 1)

    def test_cancel_204_and_ambiguous_cancel(self):
        transport = FakeTransport((204, b""))
        self.assertIsNone(broker(transport).cancel_order("order-123"))
        self.assertEqual(transport.requests[0].get_method(), "DELETE")
        transport = FakeTransport((503, b"private-secret"))
        with self.assertRaises(OrderAmbiguous):
            broker(transport).cancel_order("order-123")
        self.assertEqual(len(transport.requests), 1)

    def test_bad_credentials_error_is_sanitized(self):
        def loader():
            raise RuntimeError("private-secret")
        with self.assertRaises(BrokerError) as caught:
            PaperBroker(credential_loader=loader)
        self.assertNotIn("private-secret", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
