"""Concrete parser contracts and configuration failures, without cloud access."""

import base64
import hashlib
import hmac
import types
from datetime import datetime, timezone

from azure.core.exceptions import AzureError
from fastapi import HTTPException
import pytest

import dependencies
import main
from api import panel_operator
from jobs import sharepoint_ingestion_config as sharepoint


@pytest.mark.parametrize(("header", "ttl"), [
    ("", 3600), ("public", 3600), ("max-age=", 3600),
    ("max-age=not-a-number", 3600), ("public, max-age=120", 120),
])
def test_jwks_ttl_parser_retains_invalid_value_default(header, ttl):
    assert dependencies._parse_cache_control_ttl(header) == ttl


def test_jwks_refresh_removes_only_selected_tenant(monkeypatch):
    cache = {
        f"{tenant}|{url}": {"jwks": tenant}
        for tenant in ("selected", "other")
        for url in dependencies._jwks_urls_for_tenant(tenant).values()
    }
    monkeypatch.setattr(dependencies, "_JWKS_CACHE", cache)
    dependencies._force_refresh_jwks_cache("selected")
    dependencies._force_refresh_jwks_cache("selected")
    assert len(cache) == 2
    assert all(key.startswith("other|") for key in cache)


@pytest.mark.parametrize("cursor", ["not-separated", "a.a", "\u00e9.a"])
def test_malformed_cursor_is_422_before_secret_lookup(cursor):
    def unexpected_config(*args, **kwargs):
        raise AssertionError("malformed transport cannot need a signing secret")

    with pytest.raises(HTTPException) as caught:
        panel_operator._decode_cursor(
            cursor, expected_oid="caller", config=types.SimpleNamespace(get=unexpected_config),
        )
    assert caught.value.status_code == 422
    assert caught.value.detail == "Malformed pagination cursor."


@pytest.mark.parametrize("payload", [b"not-json", b"\xff"])
def test_signed_malformed_json_and_invalid_encoding_are_422(monkeypatch, payload):
    secret = "test-only-signing-material"
    monkeypatch.setattr(panel_operator, "_cursor_secret", lambda config: secret)
    signature = hmac.new(secret.encode(), payload, hashlib.sha256).digest()
    cursor = ".".join(base64.urlsafe_b64encode(part).decode().rstrip("=") for part in (payload, signature))
    with pytest.raises(HTTPException) as caught:
        panel_operator._decode_cursor(cursor, expected_oid="caller", config=None)
    assert caught.value.status_code == 422
    assert caught.value.detail == "Malformed pagination cursor."


@pytest.mark.parametrize("value", ["", None, "invalid", "2026-99-99"])
def test_sharepoint_invalid_timestamp_retains_epoch(value):
    assert sharepoint._as_dt(value) == datetime(1970, 1, 1, tzinfo=timezone.utc)


def test_sharepoint_iso_timestamp_retains_timezone():
    assert sharepoint._as_dt("2026-01-02T03:04:05Z") == datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.mark.parametrize("failure", [AzureError("provider failure"), RuntimeError("provider failure")])
def test_sharepoint_provider_failure_is_not_an_empty_setting(monkeypatch, failure):
    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(sharepoint, "get_config", lambda: types.SimpleNamespace(get=fail))
    with pytest.raises(type(failure)) as caught:
        sharepoint._get_config_str("SHAREPOINT_CLIENT_ID")
    assert caught.value is failure


@pytest.mark.parametrize("value", [None, "", " configured "])
def test_sharepoint_absent_and_present_values_keep_contract(monkeypatch, value):
    monkeypatch.setattr(
        sharepoint, "get_config", lambda: types.SimpleNamespace(get=lambda *args, **kwargs: value),
    )
    assert sharepoint._get_config_str("SHAREPOINT_CLIENT_ID") == (value or "").strip()


@pytest.mark.parametrize("name", ["missing/zone", "../invalid", "/absolute"])
def test_invalid_scheduler_timezone_retains_machine_fallback(monkeypatch, name):
    monkeypatch.setenv("SCHEDULER_TIMEZONE", name)
    monkeypatch.setattr(main, "get_localzone", lambda: timezone.utc)
    assert main._resolve_timezone() is timezone.utc
