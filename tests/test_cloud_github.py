"""Synthetic GitHub transport tests. No network or real credential store access."""
import base64
import os
import unittest
from unittest.mock import patch

from zerotrader.cloud_checkpoint import CheckpointError, GitCheckpointStore
from zerotrader.cloud_github import PrivateGithubCheckpointStore, STATE_REPOSITORY


TOKEN = "synthetic-private-state-token-not-real"


def metadata():
    return {"full_name": STATE_REPOSITORY, "private": True, "visibility": "private",
            "archived": False, "disabled": False}


class PrivateGithubTests(unittest.TestCase):
    def test_production_only_initial_archive_fetch_gets_longer_allowance(self):
        store = self.store()
        self.addCleanup(store.close)
        self.assertEqual(store._timeout, 30)
        self.assertEqual(store._initial_fetch_timeout, 180)
        self.assertFalse(store._initial_fetch_completed)

    def test_environment_terminal_line_ending_only_is_normalized(self):
        for framed in (TOKEN + "\r\n", "\ufeff" + TOKEN + "\r\n"):
            with patch.dict(os.environ, {"ZOH_STATE_GITHUB_TOKEN": framed}):
                self.assertEqual(PrivateGithubCheckpointStore()._token(), TOKEN)
        for invalid in (TOKEN + "\nInjected: bad", TOKEN + "\u00a0", TOKEN + "\ufeff", "\ufeff\ufeff" + TOKEN):
            with patch.dict(os.environ, {"ZOH_STATE_GITHUB_TOKEN": invalid}):
                with self.assertRaises(CheckpointError):
                    PrivateGithubCheckpointStore()._token()

    def store(self, response=None):
        return PrivateGithubCheckpointStore(credential_loader=lambda: TOKEN,
                                            metadata_transport=lambda request: metadata() if response is None else response)

    def test_only_fixed_metadata_endpoint_receives_scoped_token(self):
        def check(request):
            self.assertEqual(request.full_url, "https://api.github.com/repos/" + STATE_REPOSITORY)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(request.get_header("Authorization"), "Bearer " + TOKEN)
            return metadata()
        store = PrivateGithubCheckpointStore(credential_loader=lambda: TOKEN, metadata_transport=check)
        self.assertTrue(store.verify_private_repository())

    def test_public_or_wrong_or_disabled_repository_refuses_before_git(self):
        for change in ({"private": False}, {"visibility": "public"}, {"archived": True},
                       {"disabled": True}, {"full_name": "other/synthetic"}, {"private": None}):
            value = metadata()
            value.update(change)
            with self.subTest(change=change), patch.object(GitCheckpointStore, "_write") as write:
                with self.assertRaises(CheckpointError):
                    self.store(value).bootstrap({})
                write.assert_not_called()

    def test_metadata_failure_does_not_expose_response_or_credential(self):
        def fail(request):
            raise RuntimeError(TOKEN + " synthetic-private-response")
        store = PrivateGithubCheckpointStore(credential_loader=lambda: TOKEN, metadata_transport=fail)
        with self.assertRaises(CheckpointError) as raised:
            store.verify_private_repository()
        self.assertNotIn(TOKEN, str(raised.exception))
        self.assertNotIn("synthetic-private-response", str(raised.exception))

    def test_missing_and_header_injection_credentials_are_rejected(self):
        for token in ("", None, 42, "x" * 513, "a" * 30 + "\r\nInjected: value"):
            with self.subTest(token_type=type(token).__name__):
                store = PrivateGithubCheckpointStore(credential_loader=lambda: token)
                with self.assertRaises(CheckpointError):
                    store._git_environment()

    def test_git_has_scoped_authorization_no_broker_secrets_or_ambient_git_config(self):
        with patch.dict(os.environ, {"ZOH_STATE_GITHUB_TOKEN": "ambient-synthetic",
                         "ZOH_MAIN_PAPER_KEY": "synthetic-broker-key", "ZOH_MAIN_PAPER_SECRET": "synthetic-broker-secret",
                         "GIT_CONFIG_COUNT": "99", "GIT_CONFIG_KEY_98": "bad", "GIT_TRACE": "1"}):
            env = self.store()._git_environment()
        self.assertNotIn("ZOH_STATE_GITHUB_TOKEN", env)
        self.assertNotIn("ZOH_MAIN_PAPER_KEY", env)
        self.assertNotIn("ZOH_MAIN_PAPER_SECRET", env)
        self.assertNotIn("GIT_CONFIG_KEY_98", env)
        self.assertNotIn("GIT_TRACE", env)
        self.assertEqual(env["GIT_CONFIG_COUNT"], "3")
        self.assertEqual(env["GIT_CONFIG_KEY_0"], "http.https://github.com/" + STATE_REPOSITORY + ".git.extraheader")
        encoded = env["GIT_CONFIG_VALUE_0"].split(" ")[-1]
        self.assertEqual(base64.b64decode(encoded).decode(), "x-access-token:" + TOKEN)
        self.assertEqual(env["GIT_CONFIG_KEY_1"], "http.followRedirects")
        self.assertEqual(env["GIT_CONFIG_VALUE_1"], "false")

