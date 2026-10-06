"""Agent Landing Zone single-read: `agent-lz` label and exact key names only (no legacy fallback)."""

import types

import pytest
from tenacity import wait_none

from tools import appconfig
from tools.appconfig import AppConfigClient, build_label_selectors, key_candidates


def reader(monkeypatch, values, allow_env=False):
    if allow_env:
        monkeypatch.setenv("allow_environment_variables", "true")
    else:
        monkeypatch.delenv("allow_environment_variables", raising=False)
    client = object.__new__(AppConfigClient)
    client.client = values
    retry = AppConfigClient.get_config_with_retry.retry_with(wait=wait_none())
    client.get_config_with_retry = types.MethodType(retry, client)
    return client


def test_label_selectors_read_only_agent_lz():
    labels = [selector.label_filter for selector in build_label_selectors()]
    assert labels == ["gpt-rag-ingestion", "agent-lz", None]


def test_label_merge_ignores_legacy_gpt_rag_label():
    by_label = {
        "gpt-rag": {"SEARCH_INDEX": "legacy", "ONLY_LEGACY": "old"},
        "agent-lz": {"SEARCH_INDEX": "new"},
    }
    merged = {}
    for selector in build_label_selectors():
        merged.update(by_label.get(selector.label_filter, {}))
    assert merged == {"SEARCH_INDEX": "new"}


def test_constructor_loads_single_label_selectors(monkeypatch):
    captured = {}

    def fake_load(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setenv("APP_CONFIG_ENDPOINT", "https://example.azconfig.io")
    monkeypatch.setattr(appconfig, "_provider_load", fake_load)
    AppConfigClient()
    assert [s.label_filter for s in captured["selects"]] == ["gpt-rag-ingestion", "agent-lz", None]


@pytest.mark.parametrize("key", ["AGENTLZ_REPO_ROOT", "GPT_RAG_REPO_ROOT", "SEARCH_INDEX"])
def test_key_candidates_is_exact_key(key):
    assert key_candidates(key) == [key]


def test_agentlz_key_is_read_directly(monkeypatch):
    config = reader(monkeypatch, {"AGENTLZ_FLAG": "new", "GPT_RAG_FLAG": "old"})
    assert config.get("AGENTLZ_FLAG") == "new"


def test_legacy_key_is_not_a_fallback(monkeypatch):
    config = reader(monkeypatch, {"GPT_RAG_FLAG": "old"})
    assert config.get("AGENTLZ_FLAG", default="d") == "d"
    with pytest.raises(Exception, match="AGENTLZ_FLAG not found"):
        config.get("AGENTLZ_FLAG")


def test_environment_reads_exact_name_only(monkeypatch):
    monkeypatch.setenv("AGENTLZ_FLAG", "env-new")
    config = reader(monkeypatch, {}, allow_env=True)
    assert config.get("AGENTLZ_FLAG") == "env-new"
    assert config.get("GPT_RAG_FLAG", default="d") == "d"
