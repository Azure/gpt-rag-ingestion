"""Drive the actual API-key upload route through its direct Search SDK call."""

import base64
import logging
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock

from azure.core.exceptions import AzureError
from azure.search.documents.models import IndexingResult
from fastapi.testclient import TestClient
import pytest

import main
from jobs.sharepoint_ingestion_config import _make_chunk_key
from telemetry import audit


@pytest.fixture
def upload_route(monkeypatch):
    config = types.SimpleNamespace(get=lambda key, default=None, **kwargs: {
        "SEARCH_RAG_INDEX_NAME": "index",
        "EMBEDDINGS_VECTOR_DIMENSIONS": "2",
    }.get(key, default))
    monkeypatch.setattr(main, "app_config_client", config)
    monkeypatch.setattr(main.app, "dependency_overrides", {main.validate_api_key_header: lambda: None})
    chunking = types.ModuleType("chunking")
    chunks = [{"content": "first", "chunk_id": 0}, {"content": "second", "chunk_id": 1}]
    chunking.DocumentChunker = lambda: types.SimpleNamespace(chunk_documents=lambda data: (chunks, [], []))
    monkeypatch.setitem(sys.modules, "chunking", chunking)

    sdk = types.SimpleNamespace(upload_documents=AsyncMock())
    tools = types.ModuleType("tools")
    tools.__path__ = [str(Path(__file__).resolve().parents[1] / "tools")]
    tools.AzureOpenAIClient = lambda: types.SimpleNamespace(get_embeddings=lambda text: [0.1, 0.2])
    tools.AISearchClient = lambda: types.SimpleNamespace(get_search_client=AsyncMock(return_value=sdk))
    monkeypatch.setitem(sys.modules, "tools", tools)
    blob = types.ModuleType("tools.blob")
    blob.upload_bytes_to_container = lambda **kwargs: None
    monkeypatch.setitem(sys.modules, "tools.blob", blob)
    # Only the adapter module's module-time configuration lookup needs a stub.
    dependencies = types.ModuleType("dependencies")
    dependencies.get_config = lambda: config
    monkeypatch.setitem(sys.modules, "dependencies", dependencies)
    previous = sys.modules.get("tools.aisearch")
    monkeypatch.delitem(sys.modules, "tools.aisearch", raising=False)
    yield TestClient(main.app), sdk
    if previous is None:
        sys.modules.pop("tools.aisearch", None)


def request_body():
    return {
        "conversationId": "conversation",
        "securityUserIds": [" uploader-oid ", "anonymous", "", "00000000-0000-0000-0000-000000000000"],
        "values": [{
            "recordId": "record",
            "data": {
                "fileName": "document.txt", "contentType": "text/plain",
                "fileBase64": base64.b64encode(b"document").decode(),
            },
        }],
    }


def sdk_result(key, succeeded):
    return IndexingResult.deserialize({
        "key": key, "status": succeeded, "statusCode": 201 if succeeded else 400,
        "errorMessage": "private-search-canary",
    })


@pytest.mark.parametrize(("case", "indexed", "failed"), [
    ("success", 2, 0), ("partial", 1, 1), ("missing", 1, 1),
    ("empty", 0, 2), ("unrelated", 0, 2), ("duplicate", 0, 2),
    ("malformed", 0, 1), ("transport", 0, 1),
])
def test_direct_upload_requires_matching_confirmation(upload_route, caplog, case, indexed, failed):
    client, sdk = upload_route

    async def upload(*, documents):
        first, second = (document["id"] for document in documents)
        if case == "transport":
            raise AzureError("private-search-canary")
        return {
            "success": [sdk_result(first, True), sdk_result(second, True)],
            "partial": [sdk_result(first, True), sdk_result(second, False)],
            "missing": [sdk_result(first, True)],
            "empty": [],
            "unrelated": [sdk_result("unrequested", True)],
            "duplicate": [sdk_result(first, True), sdk_result(first, True)],
            "malformed": [object()],
        }[case]

    sdk.upload_documents.side_effect = upload
    with caplog.at_level(logging.INFO):
        response = client.post("/ingest-documents", json=request_body())
    assert response.status_code == 200
    value = response.json()["values"][0]
    assert value["recordId"] == "record"
    assert value["indexedChunks"] == indexed
    assert len(value["errors"]) == failed
    assert "private-search-canary" not in response.text + caplog.text
    documents = sdk.upload_documents.call_args.kwargs["documents"]
    assert len(documents) == 2
    for document in documents:
        assert document["parent_id"] == "/ingest/conversation/record/document.txt"
        assert document["id"] == _make_chunk_key(document["parent_id"], document["chunk_id"])
        assert document["conversationId"] == "conversation"
        assert document["metadata_security_user_ids"] == ["uploader-oid"]
        assert document["metadata_security_group_ids"] == []
        assert document["metadata_security_rbac_scope"] == ""
        assert document["contentVector"] == [0.1, 0.2]


def test_direct_upload_audit_export_failure_keeps_confirmed_result(upload_route, monkeypatch, caplog):
    client, sdk = upload_route

    async def upload(*, documents):
        return [sdk_result(document["id"], True) for document in documents]

    def fail_export(*args, **kwargs):
        raise RuntimeError("private-export-canary")

    sdk.upload_documents.side_effect = upload
    monkeypatch.setattr(audit._logger, "info", fail_export)
    response = client.post("/ingest-documents", json=request_body())
    assert response.status_code == 200
    assert response.json()["values"][0]["indexedChunks"] == 2
    assert response.json()["values"][0]["errors"] == []
    assert "Audit event export failed" in caplog.text
    assert "private-export-canary" not in response.text + caplog.text
