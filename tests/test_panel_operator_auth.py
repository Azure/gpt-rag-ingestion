"""Tests for the panel operator bearer/role validation in ``dependencies.py``
(issue #611, ADR-0004).

Mirrors the direct-testing style already used for
``validate_delegated_user_bearer`` in ``tests/test_retrieval.py``.
"""

from __future__ import annotations

import types

import pytest
from fastapi import HTTPException, Request

import dependencies as real_dependencies


def _request_with_bearer(token: str = "opaque-token") -> Request:
    auth_value = "Bearer " + token
    return Request(
        {
            "type": "http",
            "headers": [(b"authorization", auth_value.encode())],
        }
    )


def _fake_config(values: dict):
    return types.SimpleNamespace(get=lambda key, default=None, allow_none=True: values.get(key, default))


@pytest.mark.parametrize(
    ("role_cfg", "group_cfg", "expected"),
    [
        ("", "", False),
        ("PanelOperator", "", True),
        ("", "11111111-1111-1111-1111-111111111111", True),
        ("PanelOperator", "11111111-1111-1111-1111-111111111111", True),
    ],
)
def test_operator_role_or_group_configured(monkeypatch, role_cfg, group_cfg, expected):
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: _fake_config(
            {"PANEL_OPERATOR_APP_ROLE": role_cfg, "PANEL_OPERATOR_GROUP_ID": group_cfg}
        ),
    )

    assert real_dependencies.operator_role_or_group_configured() is expected


@pytest.mark.asyncio
async def test_validate_delegated_operator_bearer_rejects_app_only_token(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        return {"oid": "svc-principal", "idtyp": "app", "roles": ["PanelOperator"]}

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: _fake_config({"PANEL_OPERATOR_APP_ROLE": "PanelOperator"}),
    )

    with pytest.raises(HTTPException) as exc:
        await real_dependencies.validate_delegated_operator_bearer(_request_with_bearer())

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_validate_delegated_operator_bearer_requires_oid(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        return {"scp": "access_as_user", "idtyp": "user", "roles": ["PanelOperator"]}

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: _fake_config({"PANEL_OPERATOR_APP_ROLE": "PanelOperator"}),
    )

    with pytest.raises(HTTPException) as exc:
        await real_dependencies.validate_delegated_operator_bearer(_request_with_bearer())

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_validate_delegated_operator_bearer_rejects_missing_role(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        return {
            "oid": "user-a",
            "scp": "access_as_user",
            "idtyp": "user",
            "roles": ["SomeOtherRole"],
        }

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: _fake_config({"PANEL_OPERATOR_APP_ROLE": "PanelOperator"}),
    )

    with pytest.raises(HTTPException) as exc:
        await real_dependencies.validate_delegated_operator_bearer(_request_with_bearer())

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_validate_delegated_operator_bearer_accepts_configured_role(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        return {
            "oid": "user-a",
            "scp": "access_as_user",
            "idtyp": "user",
            "roles": ["PanelOperator"],
        }

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: _fake_config({"PANEL_OPERATOR_APP_ROLE": "PanelOperator"}),
    )

    bearer = await real_dependencies.validate_delegated_operator_bearer(_request_with_bearer())

    assert bearer.claims["oid"] == "user-a"
    assert "opaque-token" not in repr(bearer)


@pytest.mark.asyncio
async def test_validate_delegated_operator_bearer_accepts_configured_group(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        return {
            "oid": "user-a",
            "scp": "access_as_user",
            "idtyp": "user",
            "roles": [],
            "groups": ["11111111-1111-1111-1111-111111111111"],
        }

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: _fake_config(
            {"PANEL_OPERATOR_GROUP_ID": "11111111-1111-1111-1111-111111111111"}
        ),
    )

    bearer = await real_dependencies.validate_delegated_operator_bearer(_request_with_bearer())

    assert bearer.claims["oid"] == "user-a"


@pytest.mark.asyncio
async def test_validate_delegated_operator_bearer_rejects_missing_scope(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        return {"oid": "user-a", "idtyp": "user", "roles": ["PanelOperator"]}

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: _fake_config({"PANEL_OPERATOR_APP_ROLE": "PanelOperator"}),
    )

    with pytest.raises(HTTPException) as exc:
        await real_dependencies.validate_delegated_operator_bearer(_request_with_bearer())

    assert exc.value.status_code == 403
