"""Hosted/panel deployment-mode resolution (Azure/GPT-RAG ADR-0001).

`gpt-rag-ingestion` supports three deployment modes, resolved once at startup
from App Configuration (label ``gpt-rag``) and never re-read per request —
structural surface changes (which routers are mounted) require a container
restart, matching the frozen ADR-0001 contract:

* **Classic** — ``DEPLOY_HOSTED_AGENT_ORCHESTRATION=false``. Unchanged from
  pre-#592 behavior: the ingestion admin dashboard (``/dashboard``,
  ``/api/*`` admin routes) is mounted.
* **Hosted / no-panel** — ``DEPLOY_HOSTED_AGENT_ORCHESTRATION=true`` and
  ``DEPLOY_ADMINISTRATIVE_PANEL=false``. Fails closed: no admin dashboard, no
  panel-only routes, and panel Cosmos resources are never required or
  contacted.
* **Hosted / panel** — both flags ``true``. The admin dashboard and the new
  ADR-0001 panel surface (``/api/panel/*``: feedback/curation metadata and a
  dashboard overview) are mounted. Panel Cosmos resources
  (``DATABASE_ACCOUNT_NAME``/``DATABASE_NAME``) are validated eagerly and the
  process exits if they are missing, since ADR-0001 treats that as a
  release-blocking misconfiguration rather than a soft fallback.

Chat itself is never routed through this Container App in any mode — that
stays owned by Foundry-managed Conversations (see ADR-0001 and
Azure/GPT-RAG#592).
"""

from __future__ import annotations

import enum
from typing import Any


class DeploymentMode(str, enum.Enum):
    """The three ADR-0001 deployment modes for this service."""

    CLASSIC = "classic"
    HOSTED_NO_PANEL = "hosted_no_panel"
    HOSTED_PANEL = "hosted_panel"


_HOSTED_FLAG = "DEPLOY_HOSTED_AGENT_ORCHESTRATION"
_PANEL_FLAG = "DEPLOY_ADMINISTRATIVE_PANEL"

# Cosmos resource keys required when the panel surface is enabled. Reused
# by `tools/cosmosdb.py`'s generic `CosmosDBClient` for the panel's own
# feedback/curation container (see `api/panel.py`).
PANEL_COSMOS_ACCOUNT_KEY = "DATABASE_ACCOUNT_NAME"
PANEL_COSMOS_DATABASE_KEY = "DATABASE_NAME"


class PanelResourceError(RuntimeError):
    """Raised when hosted/panel mode is selected but required resources are missing.

    This is a fail-closed startup error, not a runtime/request-time error:
    the caller is expected to log the message and exit the process (matching
    the existing `_ensure_auth_or_exit` convention in `main.py`), since
    ADR-0001 treats hosted/panel Cosmos absence as a release-blocking
    misconfiguration.
    """


def _flag_enabled(config: Any, key: str) -> bool:
    """Truthy-string parsing shared with `api/retrieval.py`'s `_setting_enabled`."""
    value = config.get(key, default="false", allow_none=True)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def resolve_deployment_mode(config: Any) -> DeploymentMode:
    """Resolve the deployment mode from App Configuration.

    Both flags default to ``false`` (classic) when unset, matching
    Azure/GPT-RAG ADR-0001 and issue #592. Read once at startup — do not call
    this per-request.
    """
    hosted = _flag_enabled(config, _HOSTED_FLAG)
    if not hosted:
        return DeploymentMode.CLASSIC

    panel = _flag_enabled(config, _PANEL_FLAG)
    return DeploymentMode.HOSTED_PANEL if panel else DeploymentMode.HOSTED_NO_PANEL


def admin_surface_enabled(mode: DeploymentMode) -> bool:
    """Whether the ingestion admin dashboard (`/dashboard`, `/api/*` admin routes)
    should be mounted.

    True for CLASSIC (unchanged legacy behavior) and HOSTED_PANEL (the panel
    reuses the existing admin SPA/routes). False for HOSTED_NO_PANEL, which
    must fail closed and expose no admin/panel UI or routes.
    """
    return mode in (DeploymentMode.CLASSIC, DeploymentMode.HOSTED_PANEL)


def panel_surface_enabled(mode: DeploymentMode) -> bool:
    """Whether the new ADR-0001 panel API (`/api/panel/*`) should be mounted.

    Only true for HOSTED_PANEL. Classic mode never had these endpoints, so
    "preserve classic behavior" means they stay absent there too.
    """
    return mode is DeploymentMode.HOSTED_PANEL


def validate_panel_resources(config: Any, mode: DeploymentMode) -> None:
    """Fail closed if hosted/panel mode is selected without its Cosmos resources.

    No-op for CLASSIC and HOSTED_NO_PANEL — HOSTED_NO_PANEL must never
    require (or contact) panel Cosmos resources at all.
    """
    if mode is not DeploymentMode.HOSTED_PANEL:
        return

    missing = [
        key
        for key in (PANEL_COSMOS_ACCOUNT_KEY, PANEL_COSMOS_DATABASE_KEY)
        if not str(config.get(key, default=None, allow_none=True) or "").strip()
    ]
    if missing:
        raise PanelResourceError(
            "DEPLOY_ADMINISTRATIVE_PANEL=true requires the panel's Cosmos "
            f"resources, but the following App Configuration keys are "
            f"missing or blank: {', '.join(missing)}. Set them (label "
            "'gpt-rag') or disable DEPLOY_ADMINISTRATIVE_PANEL."
        )
