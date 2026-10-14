"""Read-only connection preflight; never initializes or executes a trader.

Only the fixed PAPER Account endpoint and fixed private repository metadata /
Contents directory endpoints are queried. No state payload is downloaded, no
Git process is run, and no secret file is created. Directory visibility does not
prove write permission, archive integrity, exclusive execution or migration.
An empty/inaccessible Contents endpoint is not treated as proof of read access.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sys
import urllib.error
import urllib.request

from .broker import PaperBroker
from .cloud_github import STATE_REPOSITORY


CODE_REPOSITORY = "metrobasicsusa-wq/zero-or-hero-codex-paper"
PAPER_ACCOUNT_URL = "https://paper-api.alpaca.markets/v2/account"
_REPOSITORY_URL = "https://api.github.com/repos/" + STATE_REPOSITORY
_CONTENTS_URL = _REPOSITORY_URL + "/contents"
_CHECKPOINT_DIRECTORY_URL = _CONTENTS_URL + "/checkpoint?ref=core-checkpoints"
_ALLOWED_URLS = frozenset((PAPER_ACCOUNT_URL, _REPOSITORY_URL, _CONTENTS_URL,
                          _CHECKPOINT_DIRECTORY_URL))
_MAX_RESPONSE = 1024 * 1024
_ERRORS = frozenset(("invalid_arguments", "execution_context_invalid",
                    "credentials_missing_or_invalid", "account_binding_unavailable",
                    "paper_read_failed", "paper_response_invalid", "account_binding_mismatch",
                    "private_repository_unverified", "state_read_failed", "state_response_invalid",
                    "unexpected_failure"))


class _Failure(Exception):
    def __init__(self, category):
        self.category = category if category in _ERRORS else "unexpected_failure"
        super().__init__(self.category)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _http_read(request, timeout):
    """Bounded GET only; provider error bodies are never read or surfaced."""
    if (request.get_method() != "GET" or request.data is not None
            or request.full_url not in _ALLOWED_URLS):
        raise ValueError("Read-only endpoint is not permitted.")
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=timeout) as response:
            if response.geturl() != request.full_url:
                raise ValueError("Redirect refused.")
            raw = response.read(_MAX_RESPONSE + 1)
            if len(raw) > _MAX_RESPONSE:
                raise ValueError("Response exceeds limit.")
            return response.status, raw
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        return status, b""


def _result():
    return {"format_version": 1, "status": "failed", "error_category": None,
            "execution_context_verified": False, "credentials_present": False,
            "paper_key_format_valid": False, "paper_secret_format_valid": False,
            "state_token_format_valid": False, "account_binding_format_valid": False,
            "paper_endpoint_verified": False, "account_binding_match": False,
            "private_repository_verified": False, "state_contents_read_verified": False,
            "checkpoint_present": False, "archive_index_present": False,
            "pending_migration": False, "remote_write_test_performed": False,
            "migration_verified": False, "trading_enabled": False}


def _secret(value, *, minimum=1):
    return (isinstance(value, str) and minimum <= len(value) <= 512
            and value.isascii() and all(33 <= ord(char) <= 126 for char in value))


def _trim_secret(value):
    # stdin-based Secret setup can append CRLF. Only outer ASCII whitespace is
    # tolerated; internal whitespace, non-ASCII and header injection still fail.
    return value.strip(" \t\r\n\v\f") if isinstance(value, str) else value


def _github_read(url, token, transport):
    if url not in (_REPOSITORY_URL, _CONTENTS_URL, _CHECKPOINT_DIRECTORY_URL):
        raise _Failure("state_read_failed")
    request = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + token,
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "zero-or-hero-read-only-preflight",
    }, method="GET")
    try:
        status, raw = transport(request, 20)
        if type(status) is not int or not isinstance(raw, bytes) or len(raw) > _MAX_RESPONSE:
            raise ValueError
        if status != 200:
            return status, None
        return status, json.loads(raw)
    except Exception:
        raise _Failure("state_read_failed") from None


def run_preflight(*, environ=None, paper_transport=None, github_transport=None):
    """Return only fixed public-safe fields. Injected transports are for tests.

    A pending_migration result can include unproven Contents access on an empty
    repository. Inspect the individual booleans; this is never deployment approval.
    No environment variable can configure an endpoint or choose a repository.
    """
    result = _result()
    env = os.environ if environ is None else environ
    try:
        if (env.get("GITHUB_ACTIONS") != "true"
                or env.get("GITHUB_REPOSITORY") != CODE_REPOSITORY
                or env.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
                or env.get("GITHUB_REF") != "refs/heads/main"):
            raise _Failure("execution_context_invalid")
        result["execution_context_verified"] = True
        key, secret, token = (_trim_secret(env.get(name)) for name in (
            "ZOH_MAIN_PAPER_KEY", "ZOH_MAIN_PAPER_SECRET", "ZOH_STATE_GITHUB_TOKEN"))
        expected = _trim_secret(env.get("ZOH_EXPECTED_ACCOUNT_SHA256"))
        result.update(paper_key_format_valid=_secret(key),
                      paper_secret_format_valid=_secret(secret),
                      state_token_format_valid=_secret(token, minimum=20),
                      account_binding_format_valid=(isinstance(expected, str)
                          and re.fullmatch(r"[0-9a-f]{64}", expected) is not None))
        if not all(result[name] for name in (
                "paper_key_format_valid", "paper_secret_format_valid", "state_token_format_valid")):
            raise _Failure("credentials_missing_or_invalid")
        if not result["account_binding_format_valid"]:
            raise _Failure("account_binding_unavailable")
        result["credentials_present"] = True

        transport = paper_transport or _http_read

        def account_only(request, timeout):
            if (request.get_method() != "GET" or request.data is not None
                    or request.full_url != PAPER_ACCOUNT_URL):
                raise ValueError("Only the PAPER account read is permitted.")
            status, raw = transport(request, timeout)
            if type(status) is not int or not isinstance(raw, bytes) or len(raw) > _MAX_RESPONSE:
                raise ValueError("Invalid account response.")
            return status, raw

        try:
            broker = PaperBroker(credential_loader=lambda: {"key": key, "secret": secret},
                                 transport=account_only)
            account = broker.account()
        except Exception:
            raise _Failure("paper_read_failed") from None
        result["paper_endpoint_verified"] = True
        account_id = account.get("id")
        if not isinstance(account_id, str) or not account_id or len(account_id) > 256:
            raise _Failure("paper_response_invalid")
        actual = hashlib.sha256(account_id.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(actual, expected):
            raise _Failure("account_binding_mismatch")
        result["account_binding_match"] = True

        git_read = github_transport or _http_read
        status, metadata = _github_read(_REPOSITORY_URL, token, git_read)
        if (status != 200 or not isinstance(metadata, dict)
                or metadata.get("full_name", "").lower() != STATE_REPOSITORY.lower()
                or metadata.get("private") is not True or metadata.get("visibility") != "private"
                or metadata.get("archived") is not False or metadata.get("disabled") is not False):
            raise _Failure("private_repository_unverified")
        result["private_repository_verified"] = True
        status, contents = _github_read(_CONTENTS_URL, token, git_read)
        if status == 404 and type(metadata.get("size")) is int and metadata["size"] == 0:
            # Empty repositories can return 404, but authorization failures can too.
            # Preserve that uncertainty rather than declaring Contents permission.
            result.update(status="pending_migration", pending_migration=True)
            return result
        if status != 200:
            raise _Failure("state_read_failed")
        if not isinstance(contents, list) or any(not isinstance(row, dict) for row in contents):
            raise _Failure("state_response_invalid")
        result["state_contents_read_verified"] = True
        status, contents = _github_read(_CHECKPOINT_DIRECTORY_URL, token, git_read)
        if status == 404:
            result.update(status="pending_migration", pending_migration=True)
            return result
        if status != 200:
            raise _Failure("state_read_failed")
        if (not isinstance(contents, list) or len(contents) >= 1000
                or any(not isinstance(row, dict) for row in contents)):
            raise _Failure("state_response_invalid")
        files = {row.get("path") for row in contents if row.get("type") == "file"
                 and isinstance(row.get("path"), str)}
        result["checkpoint_present"] = "checkpoint/core-state.json" in files
        result["archive_index_present"] = "checkpoint/archive-index.json" in files
        result["pending_migration"] = not (result["checkpoint_present"] and result["archive_index_present"])
        result["status"] = "pending_migration" if result["pending_migration"] else "passed"
        return result
    except _Failure as error:
        result["error_category"] = error.category
    except Exception:
        result["error_category"] = "unexpected_failure"
    return result


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if args:
        result = _result()
        result["error_category"] = "invalid_arguments"
    else:
        result = run_preflight()
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 1 if result["status"] == "failed" else 0
