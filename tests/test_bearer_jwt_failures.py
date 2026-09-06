"""Exercise signature verification and public failures without a live tenant."""

import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import HTTPException, Request
import jwt
import pytest

import dependencies


@pytest.fixture(scope="module")
def signing_keys():
    return [rsa.generate_private_key(public_exponent=65537, key_size=2048) for _ in range(2)]


@pytest.fixture
def bearer_boundary(monkeypatch, signing_keys):
    values = {"OAUTH_AZURE_AD_TENANT_ID": "tenant", "OAUTH_AZURE_AD_CLIENT_ID": "api"}
    monkeypatch.setattr(dependencies, "get_config", lambda: SimpleNamespace(
        get=lambda key, default=None, **kwargs: values.get(key, default),
    ))
    public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(signing_keys[0].public_key()))
    public.update(kid="key", use="sig")
    requests = []

    async def jwks(tenant, url):
        requests.append(url)
        return {"keys": [public]}

    monkeypatch.setattr(dependencies, "_get_cached_jwks", jwks)
    return requests


def request(token):
    return Request({"type": "http", "headers": [(b"authorization", f"Bearer {token}".encode())]})


def signed_token(key, **overrides):
    now = int(datetime.now(timezone.utc).timestamp())
    claims = {
        "tid": "tenant", "iss": "https://login.microsoftonline.com/tenant/v2.0",
        "aud": "api", "iat": now - 10, "exp": now + 600, "oid": "user",
    }
    claims.update(overrides)
    return jwt.encode(claims, key, algorithm="RS256", headers={"kid": "key"})


@pytest.mark.asyncio
@pytest.mark.parametrize(("overrides", "status", "detail"), [
    ({}, None, None),
    ({"iss": "https://sts.windows.net/tenant/"}, None, None),
    ({"exp": 1}, 401, "Token expired."),
    ({"aud": "other"}, 403, "Invalid token audience."),
    ({"iss": "https://invalid.example"}, 403, "Invalid token issuer."),
    ({"tid": "other"}, 401, "Invalid token (tenant mismatch)."),
    ({"exp": None}, 401, "Invalid token."),
    ({"exp": float("inf")}, 401, "Invalid token."),
    ({"iat": []}, 401, "Invalid token."),
    ({"exp": "invalid"}, 401, "Invalid token."),
])
async def test_real_jwt_claims_preserve_public_auth_outcomes(
    signing_keys, bearer_boundary, caplog, overrides, status, detail,
):
    token = signed_token(signing_keys[0], **overrides)
    if status is None:
        claims = await dependencies.validate_bearer_jwt(request(token))
        assert claims["oid"] == "user"
        assert len(bearer_boundary) == 1
        assert ("/v2.0/" in bearer_boundary[0]) == ("sts.windows.net" not in claims["iss"])
    else:
        with pytest.raises(HTTPException) as caught:
            await dependencies.validate_bearer_jwt(request(token))
        assert (caught.value.status_code, caught.value.detail) == (status, detail)
    assert token not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("token", ["malformed", "e30.e30.bad", "W10.e30.bad", "%%%.%%%.%%%"])
async def test_real_jwt_malformed_header_is_401(bearer_boundary, caplog, token):
    caplog.set_level(logging.DEBUG)
    with pytest.raises(HTTPException) as caught:
        await dependencies.validate_bearer_jwt(request(token))
    assert caught.value.status_code == 401
    assert bearer_boundary == []
    assert token not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("correct_on_attempt", [2, 3, None])
async def test_signature_rotation_and_alternate_endpoint_are_bounded(
    monkeypatch, signing_keys, bearer_boundary, correct_on_attempt,
):
    calls, invalidations = [], []

    async def jwks(tenant, url):
        calls.append(url)
        key = signing_keys[0] if len(calls) == correct_on_attempt else signing_keys[1]
        public = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
        public.update(kid="key", use="sig")
        return {"keys": [public]}

    monkeypatch.setattr(dependencies, "_get_cached_jwks", jwks)
    monkeypatch.setattr(dependencies, "_force_refresh_jwks_cache", invalidations.append)
    token = signed_token(signing_keys[0])
    if correct_on_attempt:
        assert (await dependencies.validate_bearer_jwt(request(token)))["oid"] == "user"
    else:
        with pytest.raises(HTTPException) as caught:
            await dependencies.validate_bearer_jwt(request(token))
        assert caught.value.status_code == 401
    assert len(calls) == (correct_on_attempt or 3)
    assert len(invalidations) == len(calls) - 1
    assert all("/v2.0/" in url for url in calls[:2])
    if len(calls) == 3:
        assert "/v2.0/" not in calls[2]


@pytest.mark.asyncio
@pytest.mark.parametrize("diagnostic", ["integrity", "graph-hint"])
async def test_optional_auth_diagnostic_failure_cannot_change_verification(
    monkeypatch, signing_keys, bearer_boundary, caplog, diagnostic,
):
    caplog.set_level(logging.DEBUG)
    calls = []

    def fail(*args):
        calls.append(True)
        raise RuntimeError("diagnostic sink unavailable")

    overrides = {}
    if diagnostic == "integrity":
        monkeypatch.setattr(dependencies.logging, "debug", fail)
    else:
        overrides["aud"] = "00000003-0000-0000-c000-000000000000"
        monkeypatch.setattr(dependencies.logging, "warning", fail)
    token = signed_token(signing_keys[0], **overrides)
    if diagnostic == "integrity":
        assert (await dependencies.validate_bearer_jwt(request(token)))["oid"] == "user"
    else:
        with pytest.raises(HTTPException) as caught:
            await dependencies.validate_bearer_jwt(request(token))
        assert (caught.value.status_code, caught.value.detail) == (403, "Invalid token audience.")
    assert calls
    assert len(bearer_boundary) == 1
