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
from unittest.mock import Mock


def provider(values):
    # Exercise the pinned SDK's actual mapping/merge methods without network bootstrap.
    instance = object.__new__(AzureAppConfigurationProvider)
    instance._dict = values
    instance._update_lock = threading.Lock()
    instance._trim_prefixes = []
    instance._feature_flag_enabled = False
    instance._configuration_mapper = None
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


@pytest.mark.parametrize("key", ["DATA_INGEST_APP_APIKEY", "INGESTION_APP_APIKEY"])
def test_api_key_existing_sources_and_environment_opt_in(monkeypatch, key):
    import dependencies
    from fastapi import HTTPException

    config = reader(monkeypatch, {key: "configured-key"})
    monkeypatch.setattr(dependencies, "get_config", lambda: config)
    monkeypatch.setenv(key, "environment-key")
    dependencies.validate_api_key_header("configured-key")
    with pytest.raises(HTTPException) as denied:
        dependencies.validate_api_key_header("environment-key")
    assert denied.value.status_code == 401
    monkeypatch.setenv("allow_environment_variables", "1")
    dependencies.validate_api_key_header("environment-key")


def test_api_key_provider_failure_does_not_fall_back_to_environment(monkeypatch):
    import dependencies

    class Values(dict):
        def __getitem__(self, key):
            raise RuntimeError("private-config-failure")

    config = reader(monkeypatch, Values())
    monkeypatch.setattr(dependencies, "get_config", lambda: config)
    monkeypatch.setenv("DATA_INGEST_APP_APIKEY", "environment-key")
    with pytest.raises(RuntimeError, match="private-config-failure"):
        dependencies.validate_api_key_header("environment-key")


@pytest.mark.parametrize("source", ["endpoint", "connection"])
def test_constructor_preserves_selectors_and_sdk_last_selected_value(monkeypatch, source):
    appconfig = importlib.import_module("tools.appconfig")
    monkeypatch.setenv("APP_CONFIG_ENDPOINT", "https://configuration.example.test")
    monkeypatch.delenv("allow_environment_variables", raising=False)
    monkeypatch.setenv("AZURE_APPCONFIG_CONNECTION_STRING", "private-connection")
    captured = []
    for name in ("ChainedTokenCredential", "ManagedIdentityCredential", "AzureCliCredential",
                 "AsyncChainedTokenCredential", "AsyncManagedIdentityCredential", "AsyncAzureCliCredential"):
        monkeypatch.setattr(appconfig, name, lambda *args, **kwargs: object())

    class Pages:
        etag = "fixture-page"

        def __init__(self, settings):
            self.pages = iter([settings])

        def by_page(self):
            return self

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.pages)

    class Service:
        def list_configuration_settings(self, *, key_filter, label_filter, **kwargs):
            assert key_filter == "*"
            label = None if label_filter in (None, "\0") else label_filter
            value = {"gpt-rag-ingestion": "ingestion", "gpt-rag": "base", None: "unlabelled"}[label]
            return Pages([
                ConfigurationSetting(key="SHARED", label=label, value=value),
                ConfigurationSetting(key=f"ONLY_{value.upper()}", label=label, value=value),
            ])

    def load(**kwargs):
        captured.append(kwargs)
        if source == "connection" and len(captured) == 1:
            raise RuntimeError("private-endpoint-error")
        wrapper = object.__new__(_ConfigurationClientWrapper)
        wrapper._client = Service()
        settings, _ = wrapper.load_configuration_settings(kwargs["selects"])
        instance = provider({})
        instance._dict = instance._process_configurations(settings, wrapper)
        return instance

    monkeypatch.setattr(appconfig, "load", load)
    config = appconfig.AppConfigClient()
    assert isinstance(config.client, AzureAppConfigurationProvider)
    assert len(captured) == (1 if source == "endpoint" else 2)
    assert [item.label_filter for item in captured[-1]["selects"][:2]] == ["gpt-rag-ingestion", "gpt-rag"]
    assert len(captured[-1]["selects"]) == 3
    assert captured[-1]["selects"][2].label_filter in (None, "\0")
    assert captured[0]["credential"] is config.credential
    assert config.get("SHARED") == "unlabelled"
    for value in ("ingestion", "base", "unlabelled"):
        assert config.get(f"ONLY_{value.upper()}") == value


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


@pytest.mark.parametrize("fallback", ["environment", "disabled-environment", "connection", "connection-failure"])
def test_constructor_fallback_keeps_source_contract_without_payloads(monkeypatch, caplog, fallback):
    appconfig = importlib.import_module("tools.appconfig")
    monkeypatch.setenv("APP_CONFIG_ENDPOINT", "https://configuration.example")
    monkeypatch.setenv("FALLBACK_VALUE", "environment")
    monkeypatch.delenv("allow_environment_variables", raising=False)
    if fallback in ("environment", "disabled-environment"):
        monkeypatch.delenv("AZURE_APPCONFIG_CONNECTION_STRING", raising=False)
    else:
        monkeypatch.setenv("AZURE_APPCONFIG_CONNECTION_STRING", "private-connection")
    if fallback == "environment":
        monkeypatch.setenv("allow_environment_variables", "1")
    for name in ("ChainedTokenCredential", "ManagedIdentityCredential", "AzureCliCredential",
                 "AsyncChainedTokenCredential", "AsyncManagedIdentityCredential", "AsyncAzureCliCredential"):
        monkeypatch.setattr(appconfig, name, lambda *args, **kwargs: object())
    failure = RuntimeError("private-connection-error")
    endpoint_failure = RuntimeError("private-endpoint-error")
    load = Mock(side_effect=[
        endpoint_failure,
        failure if fallback == "connection-failure" else provider({"FALLBACK_VALUE": "connection"}),
    ])
    monkeypatch.setattr(appconfig, "load", load)
    if fallback in ("connection-failure", "disabled-environment"):
        with pytest.raises(RuntimeError) as raised:
            AppConfigClient()
        assert raised.value is (failure if fallback == "connection-failure" else endpoint_failure)
        assert "fallback used:" not in caplog.text
    else:
        config = AppConfigClient()
        assert "fallback used:" in caplog.text
        assert config.get("FALLBACK_VALUE") == fallback
        if fallback == "environment":
            with pytest.raises(Exception, match="not found"):
                config.get("MISSING_FALLBACK_VALUE")
        monkeypatch.setenv("allow_environment_variables", "1")
        assert config.get("FALLBACK_VALUE") == "environment"
    assert load.call_count == (1 if fallback in ("environment", "disabled-environment") else 2)
    if fallback in ("connection", "connection-failure"):
        assert load.call_args.kwargs["connection_string"] == "private-connection"
        primary, secondary = [call.kwargs for call in load.call_args_list]
        assert set(secondary) == {"connection_string", "selects", "key_vault_options"}
        assert [(item.key_filter, item.label_filter) for item in secondary["selects"]] == [
            (item.key_filter, item.label_filter) for item in primary["selects"]
        ]
        assert secondary["key_vault_options"].credential is primary["credential"]
    assert "private-" not in caplog.text
