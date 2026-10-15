"""Synthetic API-boundary tests. No credential loader or network is used."""
import copy
import datetime as dt
import json
import unittest
import urllib.parse

from zerotrader.broker import BrokerError, OrderAmbiguous, OrderRejected, PaperBroker, ResponseIncomplete


CALL = "SPY261120C00600000"
PUT = "SPY261120P00600000"


class Transport:
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


def api(transport):
    return PaperBroker(credential_loader=lambda: {"key": "synthetic-key", "secret": "synthetic-secret"},
                       transport=transport, sleeper=lambda _: None)


def contract(symbol=CALL, **changes):
    row = {"symbol": symbol, "underlying_symbol": "SPY", "expiration_date": "2026-11-20",
           "type": "put" if symbol == PUT else "call", "size": "100", "status": "active",
           "tradable": True, "strike_price": "600", "style": "american", "open_interest": "123"}
    row.update(changes)
    return row


def order(**changes):
    row = {"symbol": CALL, "qty": 1, "side": "buy", "type": "limit", "time_in_force": "day",
           "client_order_id": "zoh-options-test", "position_intent": "buy_to_open", "limit_price": "2.50"}
    row.update(changes)
    return row


def activity(number, activity_type="OPEXP", **changes):
    row = {"id": f"20261120000000000::synthetic-{number}", "activity_type": activity_type,
           "date": "2026-11-20", "symbol": CALL, "qty": "-1", "net_amount": "0"}
    row.update(changes)
    return row


class OptionBrokerTests(unittest.TestCase):
    def test_complete_chain_keeps_raw_fields_and_explicit_date_bounds(self):
        first, second = contract(), contract(PUT)
        transport = Transport((200, {"option_contracts": [first], "next_page_token": "next"}),
                              (200, {"option_contracts": [second], "next_page_token": None}))
        values = api(transport).option_contracts("SPY", dt.date(2026, 11, 1), "2026-11-30")
        self.assertEqual(values, [first, second])
        for index, request in enumerate(transport.requests):
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.netloc, "paper-api.alpaca.markets")
            self.assertEqual(parsed.path, "/v2/options/contracts")
            self.assertEqual(request.get_method(), "GET")
            params = urllib.parse.parse_qs(parsed.query)
            self.assertEqual(params["underlying_symbols"], ["SPY"])
            self.assertEqual(params["expiration_date_gte"], ["2026-11-01"])
            self.assertEqual(params["expiration_date_lte"], ["2026-11-30"])
            self.assertEqual(params["status"], ["active"])
            self.assertNotIn("type", params)  # Both long calls and long puts.
            self.assertEqual(params.get("page_token"), ["next"] if index else None)

    def test_ineligible_contracts_do_not_short_circuit_pagination(self):
        excluded = [contract(size="150"), contract("SPY261120C00601000", tradable=False),
                    contract("SPY261120C00602000", status="inactive"),
                    contract("SPY1261120C00600000")]
        transport = Transport((200, {"option_contracts": excluded, "next_page_token": "next"}),
                              (200, {"option_contracts": [contract(PUT)]}))
        self.assertEqual(api(transport).option_contracts("SPY", "2026-11-20", "2026-11-20"), [contract(PUT)])
        self.assertEqual(len(transport.requests), 2)

    def test_invalid_chain_arguments_never_reach_transport(self):
        transport = Transport()
        client = api(transport)
        for args in [("SPY/../../account", "2026-11-01", "2026-11-20"),
                     ("SPY", "2026-11-30", "2026-11-01"),
                     ("SPY", "2026-02-30", "2026-11-01"),
                     ("SPY", "2026-11-01T00:00:00Z", "2026-11-30")]:
            with self.subTest(args=args), self.assertRaises(BrokerError):
                client.option_contracts(*args)
        self.assertEqual(transport.requests, [])

    def test_duplicate_or_nonadvancing_chain_is_not_returned_as_complete(self):
        cases = [
            [(200, {"option_contracts": [contract()], "next_page_token": "next"}),
             (200, {"option_contracts": [contract()]})],
            [(200, {"option_contracts": [], "next_page_token": "same"}),
             (200, {"option_contracts": [], "next_page_token": "same"})],
            [(200, {"option_contracts": [], "next_page_token": 3})],
        ]
        for responses in cases:
            with self.subTest(responses=responses), self.assertRaises(ResponseIncomplete):
                api(Transport(*responses)).option_contracts("SPY", "2026-11-01", "2026-11-30")

    def test_chain_malformed_or_mismatched_rows_fail_closed(self):
        changes = [{"underlying_symbol": "QQQ"}, {"expiration_date": "2026-12-20"},
                   {"expiration_date": "2026-11-19"}, {"type": "put"}, {"type": "future"},
                   {"size": "NaN"}, {"size": None}, {"tradable": "true"},
                   {"status": "unexpected"}, {"symbol": None}, {"strike_price": "NaN"},
                   {"strike_price": "599"}, {"strike_price": None},
                   {"symbol": "SPY261132C00600000"}, {"symbol": "SPY/../../account"}]
        for change in changes:
            with self.subTest(change=change), self.assertRaises(ResponseIncomplete):
                transport = Transport((200, {"option_contracts": [contract(**change)]}))
                api(transport).option_contracts("SPY", "2026-11-01", "2026-11-30")

    def test_chain_later_page_error_does_not_return_partial_chain(self):
        transport = Transport((200, {"option_contracts": [contract()], "next_page_token": "next"}),
                              (403, b"synthetic-secret"))
        with self.assertRaises(BrokerError) as caught:
            api(transport).option_contracts("SPY", "2026-11-01", "2026-11-30")
        self.assertNotIn("synthetic-secret", str(caught.exception))

    def test_contract_lookup_can_observe_an_inactive_held_contract(self):
        raw = contract(status="inactive", tradable=False)
        transport = Transport((200, raw))
        self.assertEqual(api(transport).option_contract(CALL), raw)
        self.assertEqual(transport.requests[0].full_url,
                         "https://paper-api.alpaca.markets/v2/options/contracts/" + CALL)
        with self.assertRaises(ResponseIncomplete):
            api(Transport((200, contract(PUT)))).option_contract(CALL)

    def test_indicative_quote_uses_exact_data_path_without_opra_fallback(self):
        quote = {"t": "2026-10-15T19:00:00Z", "bp": 2.4, "ap": 2.5, "bs": 10, "as": 10}
        transport = Transport((200, {"quotes": {CALL: quote}}))
        self.assertEqual(api(transport).option_latest_quote(CALL), {**quote, "feed": "indicative"})
        self.assertEqual(transport.requests[0].full_url,
                         "https://data.alpaca.markets/v1beta1/options/quotes/latest?symbols=" + CALL + "&feed=indicative")
        transport = Transport((403, b"synthetic-secret"))
        with self.assertRaises(BrokerError):
            api(transport).option_latest_quote(CALL)
        self.assertEqual(len(transport.requests), 1)
        for feed in ("opra", None, "", "sip", "INDICATIVE"):
            transport = Transport()
            with self.subTest(feed=feed), self.assertRaises(BrokerError):
                api(transport).option_latest_quote(CALL, feed)
            self.assertEqual(transport.requests, [])

    def test_option_quote_wrong_or_missing_symbol_is_incomplete(self):
        for quotes in ({}, {PUT: {}}, {CALL: {}, PUT: {}}):
            with self.subTest(quotes=quotes), self.assertRaises(ResponseIncomplete):
                api(Transport((200, {"quotes": quotes}))).option_latest_quote(CALL)
        for response in ({"quotes": {CALL: {}}, "feed": "opra"},
                         {"quotes": {CALL: {"feed": "opra"}}}):
            with self.subTest(response=response), self.assertRaises(ResponseIncomplete):
                api(Transport((200, response))).option_latest_quote(CALL)

    def test_fixed_data_path_exception_does_not_broaden_api_access(self):
        transport = Transport()
        client = api(transport)
        cases = [("GET", "/v1beta1/options/trades/latest", True, {"feed": "indicative"}),
                 ("GET", "/v1beta1/options/quotes/latest", False, {"feed": "indicative"}),
                 ("GET", "/v1beta1/options/quotes/latest", True, {"feed": "opra"}),
                 ("POST", "/v1beta1/options/quotes/latest", True, {"feed": "indicative"}),
                 ("GET", "/v2/account", True, None),
                 ("GET", "/v2/stocks/../../account/quotes/latest", True, None),
                 ("GET", "/v1beta1/options/quotes/latest?feed=opra", True, None)]
        for method, path, data, params in cases:
            with self.subTest(path=path, data=data, params=params), self.assertRaises(BrokerError):
                client._request(method, path, data=data, params=params)
        self.assertEqual(transport.requests, [])

    def test_invalid_occ_symbols_are_rejected_for_every_option_operation(self):
        transport = Transport()
        client = api(transport)
        invalid = ["SPY", "spy261120C00600000", "SPY261132C00600000", "SPY260229C00600000",
                   "SPY261120X00600000", "SPY261120C00000000", "SPY1261120C00600000",
                   "SPY261120C00600000/../../account", "SPY   261120C00600000", None]
        for symbol in invalid:
            for operation in (client.option_contract, client.option_latest_quote, client.do_not_exercise,
                              lambda value: client.submit_option_order(order(symbol=value))):
                with self.subTest(symbol=symbol, operation=operation), self.assertRaises(BrokerError):
                    operation(symbol)
        self.assertEqual(transport.requests, [])

    def test_long_entry_and_long_close_payloads_are_preserved_and_paper_only(self):
        close_market = order(side="sell", type="market", position_intent="sell_to_close")
        close_market.pop("limit_price")
        payloads = [order(), order(symbol=PUT, qty="1", limit_price="10"),
                    order(side="sell", position_intent="sell_to_close", limit_price="12.50"), close_market]
        transport = Transport(*[(200, {"id": "synthetic-order"}) for _ in payloads])
        client = api(transport)
        for payload in payloads:
            before = copy.deepcopy(payload)
            self.assertEqual(client.submit_option_order(payload), {"id": "synthetic-order"})
            self.assertEqual(payload, before)
        for request in transport.requests:
            self.assertEqual(request.full_url, "https://paper-api.alpaca.markets/v2/orders")
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(json.loads(request.data)["qty"], "1")

    def test_disallowed_option_shapes_and_budget_excess_never_reach_transport(self):
        changes = [{"qty": 0}, {"qty": 2}, {"qty": 1.0}, {"qty": True}, {"qty": "1.0"}, {"qty": "01"},
                   {"symbol": "SPY"}, {"client_order_id": "other-agent"}, {"position_intent": "sell_to_open"},
                   {"side": "sell"}, {"side": "short"}, {"type": "market"}, {"type": "stop"},
                   {"order_class": "oto"}, {"order_class": "mleg"}, {"time_in_force": "gtc"},
                   {"extended_hours": True}, {"legs": []}, {"stop_loss": {"stop_price": "1"}},
                   {"notional": "100"}, {"limit_price": "10.01"}, {"limit_price": "2.501"},
                   {"limit_price": "NaN"}, {"limit_price": "Infinity"}, {"limit_price": False}]
        transport = Transport()
        client = api(transport)
        for change in changes:
            with self.subTest(change=change), self.assertRaises(BrokerError):
                client.submit_option_order(order(**change))
        for remove in ("position_intent", "limit_price", "client_order_id"):
            payload = order()
            payload.pop(remove)
            with self.subTest(remove=remove), self.assertRaises(BrokerError):
                client.submit_option_order(payload)
        with self.assertRaises(BrokerError):
            client.submit_option_order(order(side="sell", type="market", position_intent="sell_to_close"))
        self.assertEqual(transport.requests, [])

    def test_options_cannot_bypass_the_equity_submit_validator(self):
        transport = Transport()
        client = api(transport)
        for symbol in (CALL, PUT, "F261120C00010000"):
            payload = order(symbol=symbol, order_class="oto", stop_loss={"stop_price": "1"})
            with self.subTest(symbol=symbol), self.assertRaises(BrokerError):
                client.submit_order(payload)
        self.assertEqual(transport.requests, [])

    def test_option_mutations_attempt_once_and_redact_provider_failures(self):
        for method in (lambda client: client.submit_option_order(order()), lambda client: client.do_not_exercise(CALL)):
            for status, error in ((403, OrderRejected), (422, OrderRejected), (429, OrderRejected),
                                  (500, OrderAmbiguous), (302, OrderAmbiguous)):
                transport = Transport((status, b"synthetic-secret synthetic-key"))
                with self.subTest(status=status, method=method), self.assertRaises(error) as caught:
                    method(api(transport))
                self.assertNotIn("synthetic-secret", str(caught.exception))
                self.assertEqual(caught.exception.status, status)
                self.assertEqual(len(transport.requests), 1)
            transport = Transport(RuntimeError("synthetic-secret"))
            with self.assertRaises(OrderAmbiguous) as caught:
                method(api(transport))
            self.assertTrue(caught.exception.__suppress_context__)
            self.assertNotIn("synthetic-secret", str(caught.exception))
            self.assertEqual(len(transport.requests), 1)

    def test_missing_option_order_receipt_stays_ambiguous(self):
        for body in (b"", b"not-json", {}, [], None, {"id": 7}, {"id": "bad\nid"}):
            transport = Transport((200, body))
            with self.subTest(body=body), self.assertRaises(OrderAmbiguous):
                api(transport).submit_option_order(order())
            self.assertEqual(len(transport.requests), 1)

    def test_dne_uses_exact_endpoint_and_accepts_documented_empty_success(self):
        for status, body in ((200, b""), (204, b""), (200, {"status": "accepted"})):
            transport = Transport((status, body))
            result = api(transport).do_not_exercise(CALL)
            self.assertEqual(result, body if isinstance(body, dict) else None)
            request = transport.requests[0]
            self.assertEqual(request.full_url, "https://paper-api.alpaca.markets/v2/positions/" + CALL + "/do-not-exercise")
            self.assertEqual(request.get_method(), "POST")
            self.assertIsNone(request.data)
        with self.assertRaises(OrderAmbiguous):
            api(Transport((200, b"invalid private provider message"))).do_not_exercise(CALL)

    def test_lifecycle_activities_are_complete_and_separate_from_fills(self):
        first = [activity(1, "OPEXC"), activity(2, "OPTRD", symbol="SPY", qty="100", net_amount="-60000")]
        second = [activity(3, "OPASN"), activity(4)]
        transport = Transport((200, first), (200, second), (200, []))
        rows = api(transport).options_lifecycle_activities("2026-11-19", "2026-11-22T00:00:00Z")
        self.assertEqual(rows, first + second)
        self.assertEqual(rows[1]["qty"], "100")
        for index, request in enumerate(transport.requests):
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.netloc, "paper-api.alpaca.markets")
            self.assertEqual(parsed.path, "/v2/account/activities")
            self.assertEqual(request.get_method(), "GET")
            params = urllib.parse.parse_qs(parsed.query)
            self.assertEqual(params["activity_types"], ["OPEXC,OPTRD,OPASN,OPEXP"])
            self.assertEqual(params["direction"], ["asc"])
            self.assertEqual(params["page_size"], ["100"])
            self.assertEqual(params.get("page_token"), None if not index else [first[-1]["id"] if index == 1 else second[-1]["id"]])

    def test_lifecycle_malformed_and_repeated_activities_fail_closed(self):
        for change in ({"activity_type": "FILL"}, {"activity_type": "DIV"}, {"date": None},
                       {"date": "2026-11-31"}, {"date": "2026-11-20 16:00:00"},
                       {"id": "private\nidentifier"}):
            with self.subTest(change=change), self.assertRaises(ResponseIncomplete):
                api(Transport((200, [activity(1, **change)]))).options_lifecycle_activities()
        with self.assertRaises(ResponseIncomplete):
            api(Transport((200, [activity(1)]), (200, [activity(1)]))).options_lifecycle_activities()
        transport = Transport()
        with self.assertRaises(BrokerError):
            api(transport).options_lifecycle_activities(after="2026-11-20T16:00:00")
        self.assertEqual(transport.requests, [])


if __name__ == "__main__":
    unittest.main()
