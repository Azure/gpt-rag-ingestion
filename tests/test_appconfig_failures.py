"""Real provider mapping, source precedence and startup failure boundaries."""

import importlib
import sys
import threading
import types

from azure.appconfiguration import ConfigurationSetting
from azure.appconfiguration.provider import AzureAppConfigurationProvider
from azure.appconfiguration.provider._client_manager import _ConfigurationClientWrapper
from azure.core.exceptions import AzureError
import pytest
from tenacity import wait_none

from tools.appconfig import AppConfigClient


def provider(values):
    # Exercise the pinned SDK's actual mapping/merge methods without network bootstrap.
    instance = object.__new__(AzureAppConfigurationProvider)
    instance._dict = values
    instance._update_lock = threading.Lock()
    instance._trim_prefixes = []
    instance._feature_flag_enabled = False
    return instance


def reader(monkeypatch, values):
    monkeypatch.delenv("allow_environment_variables", raising=False)
    client = object.__new__(AppConfigClient)
    client.client = provider(values)
    retry = AppConfigClient.get_config_with_retry.retry_with(wait=wait_none())
    client.get_config_with_retry = types.MethodType(retry, client)
    return client


@pytest.mark.parametrize("error", [AzureError("private provider detail"), RuntimeError("private provider detail")])
def test_selected_provider_failure_is_not_a_default(monkeypatch, caplog, error):
    class Values(dict):
        calls = 0

        def __getitem__(self, key):
            self.calls += 1
            raise error

    values = Values()
    config = reader(monkeypatch, values)
    with pytest.raises(type(error), match="private provider detail"):
        config.get("INGESTION_PROVENANCE_ENABLED", default=False, allow_none=True)
    assert values.calls == (5 if isinstance(error, AzureError) else 1)
    assert "private provider detail" not in caplog.text


def test_transient_provider_failure_retries_without_payload_logging(monkeypatch, caplog):
    class Values(dict):
        calls = 0

        def __getitem__(self, key):
            self.calls += 1
            if self.calls < 3:
                raise AzureError("private transient detail")
            return "true"

    values = Values()
    assert reader(monkeypatch, values).get("FLAG", type=bool) is True
    assert values.calls == 3
    assert "private transient detail" not in caplog.text


def test_real_missing_keys_defaults_empty_values_and_opt_in_environment_precedence(monkeypatch):
    config = reader(monkeypatch, {"SHARED": "provider", "EMPTY": "", "NUMBER": "7"})
    monkeypatch.setenv("SHARED", "environment")
    assert config.get("SHARED") == "provider"
    assert config.get("EMPTY", default="fallback") == ""
    assert config.get("NUMBER", type=int) == 7
    assert config.get("ABSENT", default="fallback") == "fallback"
    assert config.get("ABSENT", allow_none=True) is None
    with pytest.raises(Exception, match="not found"):
        config.get("ABSENT")
    monkeypatch.setenv("allow_environment_variables", "1")
    assert config.get("SHARED") == "environment"
    assert config.get("NUMBER", type=int) == 7


def test_constructor_preserves_selectors_and_sdk_last_selected_value(monkeypatch):
    appconfig = importlib.import_module("tools.appconfig")
    monkeypatch.setenv("APP_CONFIG_ENDPOINT", "https://configuration.example.test")
    monkeypatch.delenv("allow_environment_variables", raising=False)
    captured = {}
    for name in ("ChainedTokenCredential", "ManagedIdentityCredential", "AzureCliCredential",
                 "AsyncChainedTokenCredential", "AsyncManagedIdentityCredential", "AsyncAzureCliCredential"):
        monkeypatch.setattr(appconfig, name, lambda *args, **kwargs: object())

    class Service:
        def list_configuration_settings(self, *, key_filter, label_filter, **kwargs):
            assert key_filter == "*"
            label = None if label_filter in (None, "\0") else label_filter
            return [ConfigurationSetting(key="SHARED", label=label, value={
                "gpt-rag-ingestion": "ingestion", "gpt-rag": "base", None: "unlabelled",
            }[label])]

    def load(**kwargs):
        captured.update(kwargs)
        wrapper = object.__new__(_ConfigurationClientWrapper)
        wrapper._client = Service()
        settings, _ = wrapper.load_configuration_settings(kwargs["selects"], {})
        instance = provider({})
        instance._dict = instance._process_configurations(settings)
        return instance

    monkeypatch.setattr(appconfig, "load", load)
    config = appconfig.AppConfigClient()
    assert [item.label_filter for item in captured["selects"][:2]] == ["gpt-rag-ingestion", "gpt-rag"]
    assert len(captured["selects"]) == 3
    assert captured["selects"][2].label_filter in (None, "\0")
    assert captured["credential"] is config.credential
    assert config.get("SHARED") == "unlabelled"


@pytest.mark.asyncio
async def test_provider_read_failure_reaches_startup_before_scheduler_starts(monkeypatch):
    import main
    from telemetry import audit

    class Values(dict):
        def __getitem__(self, key):
            if key == "INGESTION_PROVENANCE_ENABLED":
                raise AzureError("provider unavailable")
            return super().__getitem__(key)

    config = reader(monkeypatch, Values({
        "RUN_JOBS_ON_STARTUP": "false", "DEPLOY_AGENT": "true", "DEPLOY_ADMINISTRATIVE_PANEL": "false",
    }))
    monkeypatch.setenv("REQUIRE_AUTH_ON_STARTUP", "false")
    monkeypatch.setattr(main, "get_config", lambda: config)
    monkeypatch.setattr(main, "is_azure_environment", lambda: True)
    monkeypatch.setattr(main, "_mount_admin_and_panel_surface", lambda mode: None)
    monkeypatch.setattr(main.Telemetry, "configure_monitoring", lambda *args: None)
    monkeypatch.setattr(main, "audit", audit)
    monkeypatch.setattr(audit, "_settings", None)
    monkeypatch.setattr(main, "app_config_client", None)
    monkeypatch.setattr(main, "DEPLOYMENT_MODE", None)
    starts = []
    scheduler = types.SimpleNamespace(start=lambda: starts.append(True),
                                      add_job=lambda *args, **kwargs: None, shutdown=lambda **kwargs: None)
    runtime = importlib.import_module("jobs.runtime")
    monkeypatch.setattr(main, "scheduler", scheduler)
    monkeypatch.setattr(runtime, "_scheduler", scheduler)
    registry = {}
    monkeypatch.setattr(main, "JOB_REGISTRY", registry)
    monkeypatch.setattr(runtime, "JOB_REGISTRY", registry)
    admin = types.ModuleType("api.admin")

    async def cleanup():
        pass

    admin._cleanup_old_runs = cleanup
    monkeypatch.setitem(sys.modules, "api.admin", admin)
    with pytest.raises(AzureError, match="provider unavailable"):
        async with main.lifespan(main.app):
            pass
    assert starts == []
