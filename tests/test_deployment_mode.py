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
    cfg = _FakeConfig(
        {
            "DATABASE_ACCOUNT_NAME": "acct",
            "DATABASE_NAME": "db",
            "OAUTH_AZURE_AD_TENANT_ID": "tenant-1",
            "OAUTH_AZURE_AD_CLIENT_ID": "client-1",
        }
    )
    validate_panel_resources(cfg, DeploymentMode.HOSTED_PANEL)  # must not raise


def test_validate_panel_resources_passes_with_legacy_client_id_fallback():
    """`dependencies.py`'s `_config_oauth()` falls back to the legacy
    `CLIENT_ID` key when `OAUTH_AZURE_AD_CLIENT_ID` is unset — startup
    validation must accept the same fallback instead of failing closed on
    deployments that only set the legacy key."""
    cfg = _FakeConfig(
        {
            "DATABASE_ACCOUNT_NAME": "acct",
            "DATABASE_NAME": "db",
            "OAUTH_AZURE_AD_TENANT_ID": "tenant-1",
            "CLIENT_ID": "legacy-client-1",
        }
    )
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


_COSMOS_ONLY_VALUES = {"DATABASE_ACCOUNT_NAME": "acct", "DATABASE_NAME": "db"}


@pytest.mark.parametrize(
    "values, expected_missing_key",
    [
        # Regression (Azure/GPT-RAG#592 re-review): hosted/panel previously
        # started and mounted routes with no Entra tenant/client configured
        # at all, deferring the failure to first request via
        # `require_panel_admin`'s per-request 500 — despite the ADR-0001
        # "no development-mode auth bypass" invariant being a startup-time
        # contract, not a runtime one.
        ({**_COSMOS_ONLY_VALUES}, "OAUTH_AZURE_AD_TENANT_ID"),
        ({**_COSMOS_ONLY_VALUES, "OAUTH_AZURE_AD_TENANT_ID": ""}, "OAUTH_AZURE_AD_TENANT_ID"),
        ({**_COSMOS_ONLY_VALUES, "OAUTH_AZURE_AD_TENANT_ID": "   "}, "OAUTH_AZURE_AD_TENANT_ID"),
        ({**_COSMOS_ONLY_VALUES, "OAUTH_AZURE_AD_TENANT_ID": "tenant-1"}, "OAUTH_AZURE_AD_CLIENT_ID"),
        (
            {
                **_COSMOS_ONLY_VALUES,
                "OAUTH_AZURE_AD_TENANT_ID": "tenant-1",
                "OAUTH_AZURE_AD_CLIENT_ID": "",
                "CLIENT_ID": "",
            },
            "OAUTH_AZURE_AD_CLIENT_ID",
        ),
    ],
)
def test_validate_panel_resources_fails_closed_when_entra_missing(values, expected_missing_key):
    with pytest.raises(PanelResourceError) as exc_info:
        validate_panel_resources(_FakeConfig(values), DeploymentMode.HOSTED_PANEL)
    message = str(exc_info.value)
    assert "DEPLOY_ADMINISTRATIVE_PANEL" in message
    assert expected_missing_key in message


def test_validate_panel_resources_noop_for_hosted_no_panel_never_requires_entra():
    """HOSTED_NO_PANEL exposes no admin/panel surface, so it must never
    require Entra ID configuration either — only panel Cosmos resources are
    gated, and only for HOSTED_PANEL."""
    validate_panel_resources(_FakeConfig(), DeploymentMode.HOSTED_NO_PANEL)  # must not raise
