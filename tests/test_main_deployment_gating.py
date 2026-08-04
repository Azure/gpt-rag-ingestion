"""Tests for the ADR-0001 hosted/panel mounting fix in `main.py`
(Azure/GPT-RAG#592).

Before this fix, `main.py` mounted the admin API and `/dashboard` SPA
unconditionally at *module import time*, regardless of deployment mode, and
never consumed `DEPLOY_ADMINISTRATIVE_PANEL`. The fix moves all of that
mounting into `_mount_admin_and_panel_surface(mode)`, called once from
inside `lifespan()` after the mode is resolved.

These tests import the real `main` module (confirmed side-effect-free at
import time — `app_config_client`/`DEPLOYMENT_MODE` stay `None` until
`lifespan()` runs) and drive `_mount_admin_and_panel_surface()` directly for
each `DeploymentMode`, using `importlib.reload(main)` between cases so each
test starts from a fresh `FastAPI()` app / `_panel_surface_mounted` flag.
"""

from __future__ import annotations

import importlib

import pytest

import main as main_module
from utils.deployment_mode import DeploymentMode


def _mounted_paths(app) -> set[str]:
    """Flatten `app.routes` into a set of effective path strings.

    FastAPI >=0.139 defers `include_router()` calls via a lazy
    `_IncludedRouter` wrapper instead of eagerly copying `APIRoute` objects
    into `app.routes`, so a shallow scan of `route.path` misses everything
    mounted through `app.include_router(...)`. Recurse into
    `effective_candidates()` to resolve the real (prefixed) paths.
    """
    from fastapi.routing import _IncludedRouter

    paths: set[str] = set()

    def _walk(routes) -> None:
        for route in routes:
            if isinstance(route, _IncludedRouter):
                _walk(route.effective_candidates())
                continue
            path = getattr(route, "path", None)
            if path:
                paths.add(path)

    _walk(app.routes)
    return paths


@pytest.fixture()
def fresh_main():
    """Reload `main` so each test gets an isolated `app` and mount-state flag."""
    module = importlib.reload(main_module)
    yield module
    # Leave a clean module behind for whichever test runs next.
    importlib.reload(main_module)


def test_classic_mode_mounts_admin_but_not_panel(fresh_main):
    fresh_main._mount_admin_and_panel_surface(DeploymentMode.CLASSIC)
    paths = _mounted_paths(fresh_main.app)
    # Admin API routes (jobs/schedules/files/config) come from `api.admin`;
    # `/dashboard`/`/logo.png` are only added when a built `static/` SPA
    # bundle is present on disk, which is not the case in this checkout, so
    # we assert on the router-backed API paths instead.
    assert "/api/config" in paths
    assert "/api/version" in paths
    assert not any(p and p.startswith("/api/panel") for p in paths)


def test_hosted_no_panel_mounts_neither_surface(fresh_main):
    """The core Azure/GPT-RAG#592 defect: hosted/no-panel must fail closed and
    expose neither the admin dashboard/API nor any `/api/panel/*` route."""
    fresh_main._mount_admin_and_panel_surface(DeploymentMode.HOSTED_NO_PANEL)
    paths = _mounted_paths(fresh_main.app)
    assert "/api/config" not in paths
    assert "/api/version" not in paths
    assert "/dashboard" not in paths
    assert not any(p and p.startswith("/api/panel") for p in paths)


def test_hosted_panel_mounts_both_surfaces(fresh_main):
    fresh_main._mount_admin_and_panel_surface(DeploymentMode.HOSTED_PANEL)
    paths = _mounted_paths(fresh_main.app)
    assert "/api/config" in paths
    assert "/api/version" in paths
    assert any(p == "/api/panel/status" for p in paths)
    assert any(p and p.startswith("/api/panel") for p in paths)


def test_retrieval_router_always_mounted_regardless_of_mode(fresh_main):
    """`retrieval_router` self-gates per-request (INV-002) and must remain
    unconditionally included no matter the resolved deployment mode."""
    paths_before = _mounted_paths(fresh_main.app)
    assert "/retrieve" in paths_before

    fresh_main._mount_admin_and_panel_surface(DeploymentMode.HOSTED_NO_PANEL)
    paths_after = _mounted_paths(fresh_main.app)
    assert "/retrieve" in paths_after


def test_mount_is_idempotent(fresh_main):
    """Calling the mount function twice must not raise or duplicate routes
    (guards against a future accidental re-invocation from `lifespan()`)."""
    fresh_main._mount_admin_and_panel_surface(DeploymentMode.HOSTED_PANEL)
    count_first = len(fresh_main.app.routes)

    fresh_main._mount_admin_and_panel_surface(DeploymentMode.HOSTED_PANEL)
    count_second = len(fresh_main.app.routes)

    assert count_first == count_second
