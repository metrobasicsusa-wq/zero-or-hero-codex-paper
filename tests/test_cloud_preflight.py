"""Synthetic read-only preflight tests; no network, credentials or state files."""
from contextlib import ExitStack, redirect_stdout
import hashlib
import io
import json
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

from zerotrader import cloud_preflight as preflight


ACCOUNT = "synthetic-account-id-not-real"
KEY = "synthetic-paper-key"
SECRET = "synthetic-paper-secret"
TOKEN = "synthetic-state-token-not-real"


def environment():
    return {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": preflight.CODE_REPOSITORY,
            "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_REF": "refs/heads/main",
            "ZOH_MAIN_PAPER_KEY": KEY, "ZOH_MAIN_PAPER_SECRET": SECRET,
            "ZOH_STATE_GITHUB_TOKEN": TOKEN,
            "ZOH_EXPECTED_ACCOUNT_SHA256": hashlib.sha256(ACCOUNT.encode()).hexdigest()}


def metadata():
    return {"full_name": preflight.STATE_REPOSITORY, "private": True, "visibility": "private",
            "archived": False, "disabled": False, "size": 1, "permissions": {"push": True}}


class Transport:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append(request)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        status, data = response
        return status, data if isinstance(data, bytes) else json.dumps(data).encode()


class PreflightTests(unittest.TestCase):
    def setUp(self):
        self.env = environment()
        self.paper = Transport((200, {"id": ACCOUNT, "account_number": "synthetic-private-number"}))
        self.github = Transport((200, metadata()), (200, [{"type": "file", "path": "README.md"}]),
                                (200, [{"type": "file", "path": "checkpoint/core-state.json"},
                                       {"type": "file", "path": "checkpoint/archive-index.json"}]))

    def run_check(self):
        return preflight.run_preflight(environ=self.env, paper_transport=self.paper,
                                       github_transport=self.github)

    def assert_public(self, result):
        self.assertEqual(set(result), set(preflight._result()))
        raw = json.dumps(result)
        for private in (ACCOUNT, KEY, SECRET, TOKEN, self.env.get("ZOH_EXPECTED_ACCOUNT_SHA256"),
                        "synthetic-private-number", "private-exception-body"):
            if isinstance(private, str) and private:
                self.assertNotIn(private, raw)
        self.assertFalse(result["trading_enabled"])
        self.assertFalse(result["migration_verified"])
        self.assertFalse(result["remote_write_test_performed"])

    def test_fixed_gets_only_no_credential_fallback_files_git_or_broker_mutations(self):
        with ExitStack() as stack:
            forbidden = [stack.enter_context(patch(target, side_effect=AssertionError("Forbidden operation")))
                         for target in ("zerotrader.broker.load_credentials", "builtins.open", "subprocess.run",
                                        "zerotrader.broker.PaperBroker.submit_order",
                                        "zerotrader.broker.PaperBroker.cancel_order")]
            result = self.run_check()
            for mock in forbidden:
                mock.assert_not_called()
        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["account_binding_match"])
        self.assertTrue(result["state_contents_read_verified"])
        self.assertEqual([row.full_url for row in self.paper.requests], [preflight.PAPER_ACCOUNT_URL])
        self.assertEqual([row.full_url for row in self.github.requests],
                         [preflight._REPOSITORY_URL, preflight._CONTENTS_URL, preflight._CHECKPOINT_DIRECTORY_URL])
        for request in self.paper.requests + self.github.requests:
            self.assertEqual(request.get_method(), "GET")
            self.assertIsNone(request.data)
        self.assertEqual(self.paper.requests[0].get_header("Apca-api-key-id"), KEY)
        self.assertEqual(self.github.requests[0].get_header("Authorization"), "Bearer " + TOKEN)
        self.assert_public(result)

    def test_untrusted_execution_context_stops_before_credentials_or_network(self):
        for name, value in (("GITHUB_ACTIONS", "false"), ("GITHUB_REPOSITORY", "other/repository"),
                            ("GITHUB_EVENT_NAME", "pull_request_target"), ("GITHUB_REF", "refs/heads/other")):
            with self.subTest(name=name):
                env = environment()
                env[name] = value
                result = preflight.run_preflight(environ=env, paper_transport=self.paper,
                                                 github_transport=self.github)
                self.assertEqual(result["error_category"], "execution_context_invalid")
                self.assertFalse(result["credentials_present"])
        self.assertEqual(self.paper.requests + self.github.requests, [])

    def test_missing_invalid_or_header_injection_secrets_never_reach_network(self):
        for name in ("ZOH_MAIN_PAPER_KEY", "ZOH_MAIN_PAPER_SECRET", "ZOH_STATE_GITHUB_TOKEN"):
            for bad in (None, "", "a\r\nInjected: yes", "x" * 513):
                with self.subTest(name=name, bad_type=type(bad).__name__):
                    env = environment()
                    env[name] = bad
                    result = preflight.run_preflight(environ=env, paper_transport=self.paper,
                                                     github_transport=self.github)
                    self.assertEqual(result["error_category"], "credentials_missing_or_invalid")
        self.assertEqual(self.paper.requests + self.github.requests, [])

    def test_expected_binding_must_be_preexisting_valid_hash(self):
        for bad in (None, "", ACCOUNT, "f" * 63):
            with self.subTest(bad_type=type(bad).__name__):
                self.env["ZOH_EXPECTED_ACCOUNT_SHA256"] = bad
                result = self.run_check()
                self.assertEqual(result["error_category"], "account_binding_unavailable")
        self.assertEqual(self.paper.requests, [])

    def test_binding_mismatch_never_queries_github_and_never_prints_identifiers(self):
        self.env["ZOH_EXPECTED_ACCOUNT_SHA256"] = "f" * 64
        result = self.run_check()
        self.assertEqual(result["error_category"], "account_binding_mismatch")
        self.assertEqual(self.github.requests, [])
        self.assert_public(result)

    def test_invalid_account_response_or_transport_errors_are_sanitized(self):
        for response in ((200, {}), (200, {"id": 42}), (200, b"not-json"),
                         (403, {"secret": SECRET}), RuntimeError("private-exception-body " + SECRET)):
            with self.subTest(response_type=type(response).__name__):
                self.paper = Transport(response)
                result = self.run_check()
                self.assertEqual(result["status"], "failed")
                self.assert_public(result)
        self.assertEqual(self.github.requests, [])

    def test_wrong_public_archived_or_disabled_state_repository_is_rejected(self):
        for change in ({"private": False}, {"visibility": "public"}, {"archived": True},
                       {"disabled": True}, {"full_name": "other/repo"}):
            with self.subTest(change=change):
                value = metadata()
                value.update(change)
                self.paper = Transport((200, {"id": ACCOUNT}))
                self.github = Transport((200, value))
                result = self.run_check()
                self.assertEqual(result["error_category"], "private_repository_unverified")
                self.assertEqual(len(self.github.requests), 1)
                self.assert_public(result)

    def test_empty_repository_is_pending_without_claiming_contents_permission(self):
        self.github = Transport((200, dict(metadata(), size=0)), (404, b""))
        result = self.run_check()
        self.assertEqual(result["status"], "pending_migration")
        self.assertTrue(result["pending_migration"])
        self.assertFalse(result["state_contents_read_verified"])
        self.assertFalse(result["checkpoint_present"])
        self.assert_public(result)

    def test_readable_repository_without_checkpoint_branch_is_pending(self):
        self.github = Transport((200, metadata()), (200, [{"path": "README.md", "type": "file"}]), (404, b""))
        result = self.run_check()
        self.assertEqual(result["status"], "pending_migration")
        self.assertTrue(result["state_contents_read_verified"])
        self.assertFalse(result["checkpoint_present"])

    def test_core_only_directory_is_pending_not_a_verified_migration(self):
        self.github = Transport((200, metadata()), (200, []),
                                (200, [{"path": "checkpoint/core-state.json", "type": "file"}]))
        result = self.run_check()
        self.assertTrue(result["checkpoint_present"])
        self.assertFalse(result["archive_index_present"])
        self.assertEqual(result["status"], "pending_migration")
        self.assert_public(result)

    def test_unreadable_nonempty_contents_is_failure_despite_push_metadata(self):
        for status in (403, 404, 429, 500, 302):
            with self.subTest(status=status):
                self.paper = Transport((200, {"id": ACCOUNT}))
                self.github = Transport((200, metadata()), (status, {"private": TOKEN}))
                result = self.run_check()
                self.assertEqual(result["error_category"], "state_read_failed")
                self.assertFalse(result["state_contents_read_verified"])
                self.assert_public(result)

    def test_github_response_shape_and_size_errors_are_sanitized(self):
        for response in ((200, b"not-json"), (200, {"content": TOKEN}),
                         (200, b"x" * (preflight._MAX_RESPONSE + 1)),
                         RuntimeError("private-exception-body " + TOKEN)):
            with self.subTest(response_type=type(response).__name__):
                self.paper = Transport((200, {"id": ACCOUNT}))
                self.github = Transport((200, metadata()), response)
                result = self.run_check()
                self.assertEqual(result["status"], "failed")
                self.assert_public(result)

    def test_redirect_handler_refuses_redirection_and_error_body_is_not_read(self):
        request = urllib.request.Request(preflight._REPOSITORY_URL, method="GET")
        handler = preflight._NoRedirect()
        self.assertIsNone(handler.redirect_request(request, None, 302, "", {}, "https://untrusted.invalid"))
        error = urllib.error.HTTPError(request.full_url, 302, TOKEN, {}, io.BytesIO(TOKEN.encode()))
        with patch("urllib.request.build_opener") as opener:
            opener.return_value.open.side_effect = error
            self.assertEqual(preflight._http_read(request, 1), (302, b""))
        self.assertTrue(error.fp.closed)

    def test_transport_rejects_write_or_nonallowlisted_url_before_io(self):
        with patch("urllib.request.build_opener") as opener:
            for url, method in ((preflight._REPOSITORY_URL, "POST"),
                                ("https://api.alpaca.markets/v2/account", "GET"),
                                (preflight.PAPER_ACCOUNT_URL + "?redirect=1", "GET")):
                with self.subTest(method=method, url=url), self.assertRaises(ValueError):
                    preflight._http_read(urllib.request.Request(url, method=method), 1)
            opener.assert_not_called()

    def test_success_response_from_another_url_is_rejected(self):
        request = urllib.request.Request(preflight._REPOSITORY_URL, method="GET")
        with patch("urllib.request.build_opener") as opener:
            response = opener.return_value.open.return_value.__enter__.return_value
            response.geturl.return_value = "https://untrusted.invalid/"
            response.status = 200
            response.read.return_value = b"{}"
            with self.assertRaises(ValueError):
                preflight._http_read(request, 1)
            response.read.assert_not_called()

    def test_default_http_read_bounds_success_body_before_json_parsing(self):
        request = urllib.request.Request(preflight._REPOSITORY_URL, method="GET")
        with patch("urllib.request.build_opener") as opener:
            response = opener.return_value.open.return_value.__enter__.return_value
            response.geturl.return_value = request.full_url
            response.status = 200
            response.read.return_value = b"x" * (preflight._MAX_RESPONSE + 1)
            with self.assertRaises(ValueError):
                preflight._http_read(request, 1)
            response.read.assert_called_once_with(preflight._MAX_RESPONSE + 1)

    def test_even_changed_broker_endpoint_is_rejected_before_transport(self):
        with patch("zerotrader.broker.PAPER_HOST", "https://api.alpaca.markets"):
            result = self.run_check()
        self.assertEqual(result["error_category"], "paper_read_failed")
        self.assertEqual(self.paper.requests, [])

    def test_cli_rejects_arguments_without_echo_or_secret_access(self):
        output = io.StringIO()
        with patch.object(preflight, "run_preflight") as run, redirect_stdout(output):
            code = preflight.main([SECRET])
        run.assert_not_called()
        self.assertEqual(code, 1)
        result = json.loads(output.getvalue())
        self.assertEqual(result["error_category"], "invalid_arguments")
        self.assert_public(result)

    def test_cli_prints_only_whitelisted_summary_for_pending_migration(self):
        result = preflight._result()
        result.update(status="pending_migration", pending_migration=True)
        output = io.StringIO()
        with patch.object(preflight, "run_preflight", return_value=result), redirect_stdout(output):
            code = preflight.main([])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), result)


if __name__ == "__main__":
    unittest.main()
