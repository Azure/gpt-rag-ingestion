"""Image deletion is allowed only after a complete, valid reference scan."""

import asyncio
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from azure.core.exceptions import AzureError
import pytest

from tests.test_worker_failure_boundaries import worker_module


@pytest.fixture
def images(worker_module):
    module = worker_module("multimodal_images_purger")
    worker = object.__new__(module.ImagesDeletedFilesPurger)
    worker.index_name = "index"
    worker.container_name = "images"
    worker.blob_base_url = "https://storage.example"
    worker.ai_search = SimpleNamespace(close=AsyncMock())
    return module, worker


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["complete", "late-page", "invalid-field", "invalid-url"])
async def test_reference_scan_is_complete_or_fails_before_deleting(images, stage, caplog):
    _, worker = images
    docs = [{"relatedImages": [f"https://storage.example/images/{i}"]} for i in range(1001)]
    if stage == "invalid-field":
        docs[-1]["relatedImages"] = "not-a-collection"
    elif stage == "invalid-url":
        docs[-1]["relatedImages"] = [123]

    async def stream(**kwargs):
        for doc in docs:
            yield doc
        if stage == "late-page":
            raise AzureError("private-page")

    worker.ai_search.iter_documents = stream
    worker.ai_search.search_documents = AsyncMock(return_value={"documents": docs[:1000]})
    worker._purge_unreferenced_images = AsyncMock()
    with caplog.at_level(logging.INFO):
        if stage == "complete":
            await worker.run()
            assert len(worker._purge_unreferenced_images.await_args.args[0]) == 1001
        else:
            with pytest.raises(AzureError if stage == "late-page" else ValueError):
                await worker.run()
            worker._purge_unreferenced_images.assert_not_awaited()
            assert "Completed run" not in caplog.text
    worker.ai_search.close.assert_awaited_once()
    assert "private-page" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["success", "properties", "late-list", "delete", "cancel"])
async def test_image_storage_is_async_confirmed_and_closed(images, monkeypatch, caplog, stage):
    module, worker = images
    events = []
    failure = asyncio.CancelledError() if stage == "cancel" else AzureError("private-storage")

    async def blobs():
        yield SimpleNamespace(name="keep")
        yield SimpleNamespace(name="remove")
        if stage == "late-list":
            raise failure

    async def delete(name):
        events.append(("delete", name))
        if stage in {"delete", "cancel"}:
            raise failure

    container = AsyncMock()
    container.__aenter__.return_value = container
    container.list_blobs = Mock(side_effect=blobs)
    container.delete_blob.side_effect = delete
    if stage == "properties":
        container.get_container_properties.side_effect = failure
    credential = AsyncMock()
    credential.__aenter__.return_value = credential
    managed = Mock(return_value=object())
    cli = Mock(return_value=object())
    chain = Mock(return_value=credential)
    factory = Mock(return_value=container)
    monkeypatch.setattr(module, "ManagedIdentityCredential", managed, raising=False)
    monkeypatch.setattr(module, "AzureCliCredential", cli, raising=False)
    monkeypatch.setattr(module, "ChainedTokenCredential", chain, raising=False)
    monkeypatch.setattr(module, "ContainerClient", factory, raising=False)
    monkeypatch.setattr(
        module, "BlobContainerClient", Mock(side_effect=AssertionError("synchronous SDK used")), raising=False,
    )
    with caplog.at_level(logging.INFO):
        if stage == "success":
            await worker._purge_unreferenced_images({"https://storage.example/images/keep"})
        else:
            with pytest.raises(type(failure)) as raised:
                await worker._purge_unreferenced_images({"https://storage.example/images/keep"})
            assert raised.value is failure
            assert "Purge process finished" not in caplog.text
    assert events == ([] if stage == "properties" else [("delete", "remove")])
    container.__aexit__.assert_awaited_once()
    credential.__aexit__.assert_awaited_once()
    managed.assert_called_once_with()
    cli.assert_called_once_with()
    chain.assert_called_once_with(managed.return_value, cli.return_value)
    factory.assert_called_once_with(
        account_url=worker.blob_base_url, container_name=worker.container_name, credential=credential,
    )
    assert "private-storage" not in caplog.text
