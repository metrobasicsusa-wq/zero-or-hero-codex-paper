"""Scoped private GitHub checkpoint transport; no trading or bootstrap on import.

The separate state repository must already exist and be private. Provision its
fine-grained Contents read/write token through the workflow's secret environment;
this module never reads Windows credentials or tries other authentication stores.
No network call occurs until an explicit operation. Credential values, HTTP
bodies and Git diagnostics are excluded from errors and public output.
"""
from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request

from .cloud_checkpoint import CheckpointError, GitCheckpointStore


STATE_REPOSITORY = "metrobasicsusa-wq/zero-or-hero-codex-state"
_META_URL = "https://api.github.com/repos/" + STATE_REPOSITORY


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _environment_token():
    # Secret provisioning through stdin may retain a terminal line ending.
    # Only outer ASCII whitespace is normalized; internal/header whitespace
    # and non-ASCII characters are still rejected by _token().
    return os.environ.get("ZOH_STATE_GITHUB_TOKEN", "").strip(" \t\r\n\v\f")


class PrivateGithubCheckpointStore(GitCheckpointStore):
    """Git writes are restricted to the previously selected private state repo.

    The API visibility check is a point-in-time check, not protection against an
    administrator changing visibility or resetting a branch during an operation.
    All writers must preserve append-only branches. This is not an execution lease.
    """

    def __init__(self, *, credential_loader=None, metadata_transport=None,
                 branch="core-checkpoints", git_executable="git", timeout_seconds=30):
        super().__init__("https://github.com/" + STATE_REPOSITORY + ".git",
                         branch=branch, git_executable=git_executable,
                         timeout_seconds=timeout_seconds)
        self._credential_loader = credential_loader or _environment_token
        self._metadata_transport = metadata_transport or self._http_metadata

    def _token(self):
        try:
            token = self._credential_loader()
            if (not isinstance(token, str) or not 20 <= len(token) <= 512
                    or not token.isascii() or any(c.isspace() or ord(c) < 33 for c in token)):
                raise ValueError
            return token
        except Exception:
            raise CheckpointError("Scoped private-state authentication is unavailable.") from None

    def _http_metadata(self, request):
        opener = urllib.request.build_opener(_NoRedirect())
        with opener.open(request, timeout=self._timeout) as response:
            if response.status != 200:
                raise ValueError
            body = response.read(256 * 1024 + 1)
            if len(body) > 256 * 1024:
                raise ValueError
            return json.loads(body)

    def verify_private_repository(self):
        """Metadata-only verification; returns no owner/account identifier data."""
        token = self._token()
        request = urllib.request.Request(_META_URL, headers={
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "zero-or-hero-private-state",
        }, method="GET")
        try:
            value = self._metadata_transport(request)
            if (not isinstance(value, dict) or value.get("private") is not True
                    or value.get("visibility") != "private"
                    or value.get("full_name", "").lower() != STATE_REPOSITORY.lower()
                    or value.get("archived") is not False
                    or value.get("disabled") is not False):
                raise ValueError
            return True
        except Exception:
            raise CheckpointError("The selected private state repository could not be verified.") from None

    def _git_environment(self):
        env = super()._git_environment()
        # A Git child needs only the state token's scoped HTTP authorization;
        # never pass brokerage secret environment variables through to it.
        for name in ("ZOH_STATE_GITHUB_TOKEN", "ZOH_MAIN_PAPER_KEY", "ZOH_MAIN_PAPER_SECRET",
                     "APCA_API_KEY_ID", "APCA_API_SECRET_KEY", "GH_TOKEN", "GITHUB_TOKEN",
                     "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN"):
            env.pop(name, None)
        encoded = base64.b64encode(("x-access-token:" + self._token()).encode("ascii")).decode("ascii")
        settings = {
            "http.https://github.com/" + STATE_REPOSITORY + ".git.extraheader": "AUTHORIZATION: basic " + encoded,
            "http.followRedirects": "false",
            "http.sslVerify": "true",
        }
        env["GIT_CONFIG_COUNT"] = str(len(settings))
        for index, (key, value) in enumerate(settings.items()):
            env["GIT_CONFIG_KEY_" + str(index)] = key
            env["GIT_CONFIG_VALUE_" + str(index)] = value
        return env

    def load(self):
        self.verify_private_repository()
        return super().load()

    def _write(self, state, expected_head, files=None):
        self.verify_private_repository()
        return super()._write(state, expected_head, files=files)

