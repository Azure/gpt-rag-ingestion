"""Unit tests for `utils/deployment_mode.py` (Azure/GPT-RAG#592, ADR-0001).

Covers the truth table for the three deployment modes, the admin/panel
surface gating predicates, and the fail-closed panel Cosmos resource
validation — with no imports of `main.py` or any Azure SDK required.
"""

from __future__ import annotations

import pytest

from utils.deployment_mode import (
    DeploymentMode,
    PanelResourceError,
    admin_surface_enabled,
    panel_surface_enabled,
    resolve_deployment_mode,
    validate_panel_resources,
)


class _FakeConfig:
    """Minimal stand-in for the App Configuration client used by `.get(...)`."""

    def __init__(self, values: dict | None = None) -> None:
        self._values = values or {}

    def get(self, key, default=None, allow_none=True):
        return self._values.get(key, default)


# ---------------------------------------------------------------------------
# resolve_deployment_mode
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hosted_flag, panel_flag, expected",
    [
        (None, None, DeploymentMode.CLASSIC),
        ("false", "false", DeploymentMode.CLASSIC),
        ("false", "true", DeploymentMode.CLASSIC),  # panel flag irrelevant if hosted is off
        ("true", "false", DeploymentMode.HOSTED_NO_PANEL),
        ("true", None, DeploymentMode.HOSTED_NO_PANEL),
        ("true", "true", DeploymentMode.HOSTED_PANEL),
        ("TRUE", "On", DeploymentMode.HOSTED_PANEL),  # case-insensitive, alt truthy spellings
        ("1", "yes", DeploymentMode.HOSTED_PANEL),
    ],
)
def test_resolve_deployment_mode(hosted_flag, panel_flag, expected):
    cfg = _FakeConfig(
        {
            "DEPLOY_HOSTED_AGENT_ORCHESTRATION": hosted_flag,
            "DEPLOY_ADMINISTRATIVE_PANEL": panel_flag,
        }
    )
    assert resolve_deployment_mode(cfg) is expected


def test_resolve_deployment_mode_defaults_to_classic_when_unset():
    """Both flags unset (no App Configuration keys at all) must resolve to CLASSIC."""
    assert resolve_deployment_mode(_FakeConfig()) is DeploymentMode.CLASSIC


# ---------------------------------------------------------------------------
# admin_surface_enabled / panel_surface_enabled
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode, expected",
    [
        (DeploymentMode.CLASSIC, True),
        (DeploymentMode.HOSTED_NO_PANEL, False),
        (DeploymentMode.HOSTED_PANEL, True),
    ],
)
def test_admin_surface_enabled(mode, expected):
    assert admin_surface_enabled(mode) is expected


@pytest.mark.parametrize(
    "mode, expected",
    [
        (DeploymentMode.CLASSIC, False),
        (DeploymentMode.HOSTED_NO_PANEL, False),
        (DeploymentMode.HOSTED_PANEL, True),
    ],
)
def test_panel_surface_enabled(mode, expected):
    assert panel_surface_enabled(mode) is expected


def test_hosted_no_panel_gets_neither_surface():
    """HOSTED_NO_PANEL must fail closed: zero admin routes, zero panel routes."""
    mode = DeploymentMode.HOSTED_NO_PANEL
    assert admin_surface_enabled(mode) is False
    assert panel_surface_enabled(mode) is False


# ---------------------------------------------------------------------------
# validate_panel_resources
# ---------------------------------------------------------------------------


def test_validate_panel_resources_noop_for_classic():
    validate_panel_resources(_FakeConfig(), DeploymentMode.CLASSIC)  # must not raise


def test_validate_panel_resources_noop_for_hosted_no_panel():
    """HOSTED_NO_PANEL must never require (or contact) panel Cosmos resources."""
    validate_panel_resources(_FakeConfig(), DeploymentMode.HOSTED_NO_PANEL)  # must not raise


def test_validate_panel_resources_passes_when_configured():
    cfg = _FakeConfig({"DATABASE_ACCOUNT_NAME": "acct", "DATABASE_NAME": "db"})
    validate_panel_resources(cfg, DeploymentMode.HOSTED_PANEL)  # must not raise


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"DATABASE_ACCOUNT_NAME": "acct"},
        {"DATABASE_NAME": "db"},
        {"DATABASE_ACCOUNT_NAME": "", "DATABASE_NAME": "db"},
        {"DATABASE_ACCOUNT_NAME": "acct", "DATABASE_NAME": "   "},
    ],
)
def test_validate_panel_resources_fails_closed_when_missing(values):
    with pytest.raises(PanelResourceError) as exc_info:
        validate_panel_resources(_FakeConfig(values), DeploymentMode.HOSTED_PANEL)
    assert "DEPLOY_ADMINISTRATIVE_PANEL" in str(exc_info.value)
