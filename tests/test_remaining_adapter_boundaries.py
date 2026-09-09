"""Expected parser/transport recovery must not conceal programming defects."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from azure.core.exceptions import AzureError
import pytest
import requests

from tests.test_worker_failure_boundaries import worker_module


@pytest.mark.parametrize("method", ["stream", "collect", "subsite", "drive"])
@pytest.mark.parametrize("failure", [requests.ConnectionError("private-graph"), RuntimeError("private-graph")])
def test_legacy_graph_only_recovers_expected_optional_transport_failures(worker_module, method, failure, caplog):
    module = worker_module("sharepoint", "tools")
    worker = module.SharePointMetadataStreamer()
    worker._are_required_variables_missing = lambda: False
    worker._get_site_and_drive_ids = lambda *args, **kwargs: ("site", "drive")
    worker._msgraph_auth = lambda: None
    worker._make_ms_graph_request = Mock(side_effect=failure)

    def call():
        if method == "stream":
            return list(worker.stream_file_metadata("example.org", "site", "drive", folder_regex=".*"))
        if method == "collect":
            return worker._get_files("site", "drive", [])
        if method == "subsite":
            return worker._get_sub_site("site", "child")
        return worker._get_drive_id("site", "drive")

    if isinstance(failure, RuntimeError) or method == "drive":
        with pytest.raises(type(failure)) as raised:
            call()
        assert raised.value is failure
    else:
        assert call() == (None if method == "subsite" else [])
        assert "ConnectionError" in caplog.text
    assert "private-graph" not in caplog.text


@pytest.mark.parametrize("value", [None, "invalid", "https://[private-invalid"])
def test_invalid_blob_url_has_safe_existing_error_type(worker_module, value, caplog):
    module = worker_module("blob", "tools")
    with pytest.raises(OSError) as raised:
        module.BlobClient(value, credential=object())
    assert "private-invalid" not in caplog.text + str(raised.value)


def test_blob_url_parser_defect_is_not_invalid_input(worker_module, monkeypatch):
    module = worker_module("blob", "tools")
    monkeypatch.setattr(module, "urlparse", Mock(side_effect=RuntimeError("defect")))
    with pytest.raises(RuntimeError, match="defect"):
        module.BlobClient("https://storage.example/container/blob", credential=object())


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["sdk", "json", "unicode", "defect", "valid"])
async def test_curation_log_read_only_recovers_expected_storage_or_encoding_failures(worker_module, stage, caplog):
    module = worker_module("corpus_curation_store", "tools")
    store = module.CorpusCurationStore()

    async def blobs(**kwargs):
        yield SimpleNamespace(name="worker/files/private-log.json")

    blob = SimpleNamespace(download_blob=AsyncMock(return_value=SimpleNamespace(
        readall=AsyncMock(return_value={"json": b"invalid", "unicode": b"\xff"}.get(stage, b'{"blocked":true}')),
    )))
    if stage in {"sdk", "defect"}:
        blob.download_blob.side_effect = AzureError("private-download") if stage == "sdk" else RuntimeError("defect")
    store._blob_service = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(
        list_blobs=blobs, get_blob_client=lambda name: blob,
    ))
    if stage == "defect":
        with pytest.raises(RuntimeError, match="defect"):
            _ = [item async for item in store._iter_file_logs()]
    else:
        items = [item async for item in store._iter_file_logs()]
        assert len(items) == (1 if stage == "valid" else 0)
        if stage != "valid":
            assert "could not read" in caplog.text
    assert "private-" not in caplog.text
