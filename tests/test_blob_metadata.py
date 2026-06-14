"""Tests for `_extract_custom_metadata` in `jobs.blob_storage_indexer`.

The helper turns the raw blob metadata mapping (lowercased by Azure Storage)
into the `custom_metadata` collection that gets stamped onto every AI Search
document. These tests pin down its normalization rules so reserved security
keys never leak into the user-facing field and so empty/whitespace values are
dropped before they reach the index.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def _load_extractor():
    """Load `_extract_custom_metadata` without importing the full module.

    `jobs.blob_storage_indexer` pulls in heavy Azure SDK and app dependencies
    at import time. We only need the pure helper, so we execute the module
    file with the first-party imports neutralised through lightweight stubs.
    """
    root = Path(__file__).resolve().parents[1]
    module_path = root / "jobs" / "blob_storage_indexer.py"

    stubs = {
        "dependencies": {"get_config": lambda *a, **k: None},
        "tools": {},
        "tools.credentials": {"get_azure_client_id": lambda *a, **k: None},
        "chunking": {"DocumentChunker": object},
        "chunking.chunker_factory": {"ChunkerFactory": object},
        "utils": {},
        "utils.file_utils": {"_safe_delete": lambda *a, **k: None},
    }
    for name, attrs in stubs.items():
        if name in sys.modules:
            continue
        module = types.ModuleType(name)
        for attr, value in attrs.items():
            setattr(module, attr, value)
        sys.modules[name] = module

    spec = importlib.util.spec_from_file_location(
        "blob_storage_indexer_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module._extract_custom_metadata


extract = _load_extractor()


def test_extracts_plain_metadata_preserving_lowercase():
    result = extract({"department": "finance", "owner": "alice"})

    assert sorted(result, key=lambda item: item["key"]) == [
        {"key": "department", "value": "finance"},
        {"key": "owner", "value": "alice"},
    ]


def test_returns_empty_for_none_or_empty():
    assert extract(None) == []
    assert extract({}) == []


def test_filters_all_reserved_security_keys():
    meta = {
        "metadata_security_user_ids": "u1",
        "metadata_security_group_ids": "g1",
        "metadata_security_id": "legacy",
        "metadata_security_rbac_scope": "/subscriptions/x",
        "department": "finance",
    }

    result = extract(meta)

    assert result == [{"key": "department", "value": "finance"}]


def test_drops_none_empty_and_whitespace_only_values():
    meta = {
        "department": "finance",
        "missing": None,
        "blank": "",
        "spaces": "   ",
    }

    result = extract(meta)

    assert result == [{"key": "department", "value": "finance"}]


def test_preserves_numeric_strings_verbatim():
    result = extract({"version": "42"})

    assert result == [{"key": "version", "value": "42"}]


def test_trims_and_lowercases_keys():
    result = extract({"  Department  ": "Finance"})

    assert result == [{"key": "department", "value": "Finance"}]


def test_drops_entries_whose_key_is_empty_after_trim():
    result = extract({"   ": "value"})

    assert result == []
