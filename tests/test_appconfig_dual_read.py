"""Agent Landing Zone R14 dual-read: `agent-lz` label and `AGENTLZ_` keys win, legacy names fall back."""

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


def test_label_selectors_put_agent_lz_after_legacy_label():
    # The provider's last matching selection wins, so agent-lz must follow gpt-rag.
    labels = [selector.label_filter for selector in build_label_selectors()]
    assert labels == ["gpt-rag-ingestion", "gpt-rag", "agent-lz", None]


def test_label_merge_prefers_agent_lz_and_falls_back_to_gpt_rag():
    by_label = {
        "gpt-rag": {"SEARCH_INDEX": "legacy", "ONLY_LEGACY": "old"},
        "agent-lz": {"SEARCH_INDEX": "new"},
    }
    merged = {}
    for selector in build_label_selectors():
        merged.update(by_label.get(selector.label_filter, {}))
    assert merged == {"SEARCH_INDEX": "new", "ONLY_LEGACY": "old"}


def test_constructor_loads_with_dual_label_selectors(monkeypatch):
    captured = {}

    def fake_load(**kwargs):
        captured.update(kwargs)
        return {}

    monkeypatch.setenv("APP_CONFIG_ENDPOINT", "https://example.azconfig.io")
    monkeypatch.setattr(appconfig, "load", fake_load)
    AppConfigClient()
    assert [s.label_filter for s in captured["selects"]] == ["gpt-rag-ingestion", "gpt-rag", "agent-lz", None]


@pytest.mark.parametrize(
    "key,expected",
    [
        ("AGENTLZ_REPO_ROOT", ["AGENTLZ_REPO_ROOT", "GPT_RAG_REPO_ROOT"]),
        ("GPT_RAG_REPO_ROOT", ["AGENTLZ_REPO_ROOT", "GPT_RAG_REPO_ROOT"]),
        ("SEARCH_INDEX", ["SEARCH_INDEX"]),
    ],
)
def test_key_candidates_order(key, expected):
    assert key_candidates(key) == expected


@pytest.mark.parametrize("requested", ["AGENTLZ_FLAG", "GPT_RAG_FLAG"])
def test_agentlz_key_wins_over_legacy_key(monkeypatch, requested):
    config = reader(monkeypatch, {"AGENTLZ_FLAG": "new", "GPT_RAG_FLAG": "old"})
    assert config.get(requested) == "new"


@pytest.mark.parametrize("requested", ["AGENTLZ_FLAG", "GPT_RAG_FLAG"])
def test_legacy_key_is_fallback(monkeypatch, requested):
    config = reader(monkeypatch, {"GPT_RAG_FLAG": "old"})
    assert config.get(requested) == "old"


def test_missing_both_keys_uses_default(monkeypatch):
    config = reader(monkeypatch, {})
    assert config.get("AGENTLZ_FLAG", default="d") == "d"
    with pytest.raises(Exception, match="AGENTLZ_FLAG not found"):
        config.get("AGENTLZ_FLAG")


def test_environment_agentlz_key_wins_when_enabled(monkeypatch):
    monkeypatch.setenv("AGENTLZ_FLAG", "env-new")
    monkeypatch.setenv("GPT_RAG_FLAG", "env-old")
    config = reader(monkeypatch, {"GPT_RAG_FLAG": "store-old"}, allow_env=True)
    assert config.get("GPT_RAG_FLAG") == "env-new"
