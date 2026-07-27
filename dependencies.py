"""
Provides dependencies for API calls.

This project uses:
- API key auth for internal ingestion calls (X-API-KEY), except `/ingest-documents`
- JWT bearer validation for end-user chat uploads (`POST /ingest-documents` only)
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
import jwt
from fastapi import Depends, Header, HTTPException, Request
from fastapi.security import APIKeyHeader

from tools.appconfig import AppConfigClient

__config: AppConfigClient | None = None


def get_config(action: str | None = None) -> AppConfigClient:
    global __config
    if action == "refresh" or __config is None:
        __config = AppConfigClient()
    return __config


def validate_api_key_header(x_api_key: str = Depends(APIKeyHeader(name="X-API-KEY"))):
    cfg = get_config()
    # Accept the canonical infra-provided key first (matches `${app.canonical_name}_APIKEY`
    # generated from canonical_name=DATA_INGEST_APP in main.parameters.json), with the
    # legacy alias as a fallback so older environments keep working.
    expected_keys = [
        cfg.get("DATA_INGEST_APP_APIKEY", default="", allow_none=True),
        cfg.get("INGESTION_APP_APIKEY", default="", allow_none=True),
    ]
    expected_keys = [k for k in expected_keys if k]
    if not expected_keys or x_api_key not in expected_keys:
        logging.error("Invalid API key. You must provide a valid API key in the X-API-KEY header.")
        raise HTTPException(status_code=401, detail="Invalid API key.")


def _get_bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization") or request.headers.get("authorization")
    if not auth:
        raise HTTPException(status_code=401, detail="Missing Authorization header.")
    parts = auth.split()
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(status_code=401, detail="Invalid Authorization header format. Expected 'Bearer <token>'.")
    return parts[1].strip()


def _parse_cache_control_ttl(cache_control_header: str) -> int:
    if not cache_control_header:
        return 3600
    for part in cache_control_header.split(","):
        part = part.strip()
        if part.startswith("max-age="):
            try:
                return int(part.split("=")[1])
            except Exception:
                return 3600
    return 3600


def _jwks_urls_for_tenant(tenant_id: str) -> Dict[str, str]:
    return {
        "v1": f"https://login.microsoftonline.com/{tenant_id}/discovery/keys",
        "v2": f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys",
    }


_JWKS_CACHE: Dict[str, Dict[str, Any]] = {}


async def _get_cached_jwks(tenant_id: str, jwks_url: str) -> Dict[str, Any]:
    cache_key = f"{tenant_id}|{jwks_url}"
    cached = _JWKS_CACHE.get(cache_key)
    if cached and cached.get("expires_at") and cached["expires_at"] > datetime.now():
        return cached["jwks"]

    async with httpx.AsyncClient() as client:
        resp = await client.get(jwks_url, timeout=10)
        resp.raise_for_status()
        jwks = resp.json()
        ttl = _parse_cache_control_ttl(resp.headers.get("cache-control", ""))

    _JWKS_CACHE[cache_key] = {"jwks": jwks, "expires_at": datetime.now() + timedelta(seconds=ttl)}
    return jwks


def _force_refresh_jwks_cache(tenant_id: str) -> None:
    try:
        for url in _jwks_urls_for_tenant(tenant_id).values():
            _JWKS_CACHE.pop(f"{tenant_id}|{url}", None)
    except Exception:
        pass


def _config_oauth() -> Tuple[str, str]:
    cfg = get_config()
    tenant_id = cfg.get("OAUTH_AZURE_AD_TENANT_ID", default=None, allow_none=True)
    client_id = cfg.get("OAUTH_AZURE_AD_CLIENT_ID", default=None, allow_none=True)
    if not client_id:
        client_id = cfg.get("CLIENT_ID", default=None, allow_none=True)
    if not tenant_id or not client_id:
        raise HTTPException(
            status_code=500,
            detail="Authentication not configured (missing OAUTH_AZURE_AD_TENANT_ID / OAUTH_AZURE_AD_CLIENT_ID).",
        )
    return str(tenant_id), str(client_id)


def _token_integrity_diagnostics(token: str) -> None:
    token = (token or "").strip()
    parts = token.split(".") if token else []
    segs = len(parts)
    fp = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12] if token else "<empty>"
    if segs != 3:
        logging.warning("[auth] Unsupported token format segments=%d fp=%s", segs, fp)
        return

    # Helpful when proxies mutate the token (URL encoding / truncation)
    try:
        b64url_re = re.compile(r"^[A-Za-z0-9_-]+$")

        def _b64url_decode_len(seg: str) -> int:
            padded = seg + "=" * (-len(seg) % 4)
            return len(base64.urlsafe_b64decode(padded.encode("utf-8")))

        h, p, s = parts
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(
                "[auth] Token base64url chars header=%s payload=%s signature=%s fp=%s",
                bool(b64url_re.match(h)),
                bool(b64url_re.match(p)),
                bool(b64url_re.match(s)),
                fp,
            )
            logging.debug(
                "[auth] Token decoded lens header=%d payload=%d signature=%d fp=%s",
                _b64url_decode_len(h),
                _b64url_decode_len(p),
                _b64url_decode_len(s),
                fp,
            )
    except Exception:
        # Do not fail auth due to diagnostics
        pass


def _find_jwk(jwks: Dict[str, Any], kid: Optional[str], x5t: Optional[str]) -> Optional[Dict[str, Any]]:
    keys = (jwks or {}).get("keys", []) or []
    matches: List[Dict[str, Any]] = []
    for k in keys:
        if kid and k.get("kid") == kid:
            matches.append(k)
        elif x5t and k.get("x5t") == x5t:
            matches.append(k)
    if not matches:
        return None
    sig_matches = [m for m in matches if (m.get("use") == "sig" or "verify" in (m.get("key_ops") or []))]
    return sig_matches[0] if sig_matches else matches[0]


async def validate_bearer_jwt(
    request: Request,
    expected_audiences: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Validate Azure AD access token from `Authorization: Bearer <token>`.
    Handles v2 and (some) v1 issuers.
    Returns the verified decoded claims.
    """
    token = _get_bearer_token(request)
    _token_integrity_diagnostics(token)

    tenant_id, client_id = _config_oauth()
    if expected_audiences is None:
        expected_audiences = [client_id, f"api://{client_id}"]
    expected_audiences = [
        str(audience).strip()
        for audience in expected_audiences
        if str(audience).strip()
    ]
    if not expected_audiences:
        raise HTTPException(
            status_code=500,
            detail="Authentication not configured (missing token audience).",
        )
    issuer_candidates = [
        f"https://login.microsoftonline.com/{tenant_id}/v2.0",
        f"https://sts.windows.net/{tenant_id}/",
    ]

    # Read unverified claims for routing + clearer diagnostics (never trust for auth decisions)
    try:
        unverified = jwt.decode(token, options={"verify_signature": False})
    except Exception:
        unverified = {}

    # Graph token hint (common 401 cause)
    try:
        graph_aud = "00000003-0000-0000-c000-000000000000"
        aud_uv = unverified.get("aud")
        if aud_uv == graph_aud or (isinstance(aud_uv, list) and graph_aud in aud_uv):
            logging.warning(
                "[auth] Token audience indicates Microsoft Graph. Client must request an access token for this API (aud=%s).",
                graph_aud,
            )
    except Exception:
        pass

    # Tenant mismatch guardrail (unverified but high-signal)
    token_tid = unverified.get("tid")
    if token_tid and str(token_tid) != str(tenant_id):
        raise HTTPException(status_code=401, detail="Invalid token (tenant mismatch).")

    jwks_urls = _jwks_urls_for_tenant(tenant_id)
    token_iss = unverified.get("iss")
    primary_jwks_url = jwks_urls["v1"] if (isinstance(token_iss, str) and token_iss.startswith("https://sts.windows.net/")) else jwks_urls["v2"]

    # Header -> select key
    try:
        hdr = jwt.get_unverified_header(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token.")
    kid = hdr.get("kid")
    x5t = hdr.get("x5t")
    alg = hdr.get("alg")
    if alg and alg != "RS256":
        raise HTTPException(status_code=401, detail="Invalid token.")
    if not kid and not x5t:
        raise HTTPException(status_code=401, detail="Invalid token.")

    async def _try_validate(jwks_url: str) -> Tuple[Optional[Dict[str, Any]], Optional[Exception]]:
        jwks = await _get_cached_jwks(tenant_id, jwks_url)
        jwk = _find_jwk(jwks, kid=kid, x5t=x5t)
        if not jwk:
            return None, KeyError("key-not-found")
        public_key = jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(jwk))

        last_err: Optional[Exception] = None
        for iss in issuer_candidates:
            try:
                claims = jwt.decode(
                    token,
                    public_key,
                    algorithms=["RS256"],
                    issuer=iss,
                    audience=expected_audiences,
                    options={"require": ["exp", "iat"]},
                )
                return claims, None
            except jwt.InvalidIssuerError as e:
                last_err = e
                continue
            except Exception as e:
                last_err = e
                break
        return None, last_err

    decoded, err = await _try_validate(primary_jwks_url)
    if decoded is None and isinstance(err, jwt.InvalidSignatureError):
        _force_refresh_jwks_cache(tenant_id)
        decoded, err = await _try_validate(primary_jwks_url)

    if decoded is None and isinstance(err, jwt.InvalidSignatureError):
        alt = jwks_urls["v1"] if primary_jwks_url == jwks_urls["v2"] else jwks_urls["v2"]
        _force_refresh_jwks_cache(tenant_id)
        decoded, err = await _try_validate(alt)

    if decoded is None:
        if isinstance(err, jwt.ExpiredSignatureError):
            raise HTTPException(status_code=401, detail="Token expired.")
        if isinstance(err, jwt.InvalidAudienceError):
            raise HTTPException(status_code=403, detail="Invalid token audience.")
        if isinstance(err, jwt.InvalidIssuerError):
            raise HTTPException(status_code=403, detail="Invalid token issuer.")
        raise HTTPException(status_code=401, detail="Invalid token.")

    return decoded


def get_user_id_from_claims(claims: Dict[str, Any]) -> str:
    user_id = (claims.get("oid") or claims.get("sub") or "").strip()
    if not user_id:
        raise HTTPException(status_code=403, detail="Token missing user identifier (oid/sub).")
    return user_id


@dataclass(frozen=True)
class ValidatedUserBearer:
    """A validated delegated-user bearer kept out of repr and logs."""

    access_token: str = field(repr=False)
    claims: Dict[str, Any] = field(repr=False)


async def validate_delegated_user_bearer(request: Request) -> ValidatedUserBearer:
    """Validate and return a delegated user token for native downstream trimming."""

    config = get_config()
    token_audience = config.get(
        "HOSTED_RETRIEVAL_TOKEN_AUDIENCE",
        default=None,
        allow_none=True,
    )
    if not isinstance(token_audience, str) or not token_audience.strip():
        raise HTTPException(
            status_code=500,
            detail=(
                "Hosted retrieval authentication not configured "
                "(missing HOSTED_RETRIEVAL_TOKEN_AUDIENCE)."
            ),
        )

    access_token = _get_bearer_token(request)
    claims = await validate_bearer_jwt(
        request,
        expected_audiences=[token_audience.strip()],
    )

    oid = claims.get("oid")
    scopes = claims.get("scp")
    identity_type = claims.get("idtyp")
    if not isinstance(oid, str) or not oid.strip():
        raise HTTPException(status_code=403, detail="Token missing user object identifier.")
    if not isinstance(scopes, str) or not scopes.strip():
        raise HTTPException(status_code=403, detail="Delegated user token required.")
    if identity_type is not None and identity_type != "user":
        raise HTTPException(status_code=403, detail="Delegated user token required.")

    return ValidatedUserBearer(access_token=access_token, claims=claims)


def handle_exception(exception: Exception, status_code: int = 500):
    logging.error(exception, stack_info=True, exc_info=True)
    raise HTTPException(status_code=status_code, detail=str(exception)) from exception
