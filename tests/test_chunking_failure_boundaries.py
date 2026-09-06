"""Real chunking/parser boundaries preserve failures without exposing payloads."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import logging
from pathlib import Path
import sys
import types
from unittest.mock import Mock
import zipfile

from azure.core.exceptions import AzureError
import fitz
import httpx
import openai
from PIL import Image
from pypdf import PdfReader, PdfWriter
import pytest
import requests


CANARY = "private-chunking-failure-canary"


@pytest.fixture
def modules(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    namespace = "_chunking_failure_tests"
    for name, path in [
        (namespace, root),
        (f"{namespace}.chunking", root / "chunking"),
        (f"{namespace}.chunking.chunkers", root / "chunking" / "chunkers"),
        ("utils", root / "utils"),
        ("tools", root / "tools"),
    ]:
        package = types.ModuleType(name)
        package.__path__ = [str(path)]
        monkeypatch.setitem(sys.modules, name, package)

    config = types.SimpleNamespace(get=lambda key, default=None: default)
    dependencies = types.ModuleType("dependencies")
    dependencies.get_config = lambda: config
    monkeypatch.setitem(sys.modules, "dependencies", dependencies)
    tools = sys.modules["tools"]
    tools.AzureOpenAIClient = Mock()
    tools.GptTokenEstimator = Mock()
    tools.BlobClient = Mock()
    tools.DocumentIntelligenceClient = type("DocumentIntelligenceClient", (), {})
    tools.ContentUnderstandingClient = type("ContentUnderstandingClient", (), {})

    def load(name, relative):
        spec = importlib.util.spec_from_file_location(name, root / relative)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    files = load("utils.file_utils", "utils/file_utils.py")
    sys.modules["utils"].get_filename_from_data = files.get_filename_from_data
    factory = types.ModuleType(f"{namespace}.chunking.chunker_factory")
    factory.ChunkerFactory = Mock()
    monkeypatch.setitem(sys.modules, factory.__name__, factory)
    load(f"{namespace}.chunking.exceptions", "chunking/exceptions.py")
    base = load(f"{namespace}.chunking.chunkers.base_chunker", "chunking/chunkers/base_chunker.py")
    analysis = load(
        f"{namespace}.chunking.chunkers.doc_analysis_chunker",
        "chunking/chunkers/doc_analysis_chunker.py",
    )
    multimodal = load(
        f"{namespace}.chunking.chunkers.multimodal_chunker",
        "chunking/chunkers/multimodal_chunker.py",
    )
    document = load(f"{namespace}.chunking.document_chunking", "chunking/document_chunking.py")
    figures = load("tools.figure_extraction", "tools/figure_extraction.py")
    return types.SimpleNamespace(
        files=files, base=base, analysis=analysis, multimodal=multimodal,
        document=document, figures=figures, factory=factory,
    )


def pdf_bytes(pages=1):
    writer = PdfWriter()
    for _ in range(pages):
        writer.add_blank_page(width=144, height=144)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def analysis_boundary(modules, data=None):
    boundary = object.__new__(modules.analysis.DocAnalysisChunker)
    boundary.filename = "document.pdf"
    boundary.extension = "pdf"
    boundary.document_bytes = pdf_bytes()
    boundary.data = data or {}
    boundary.max_pages_per_analysis = 1
    boundary._analysis_client = Mock()
    return boundary


def test_document_errors_remain_explicit_and_do_not_expose_upstream_payload(modules, caplog):
    modules.factory.ChunkerFactory.return_value.get_chunker.side_effect = RuntimeError(CANARY)
    chunks, errors, warnings = modules.document.DocumentChunker().chunk_documents(
        {"fileName": "document.pdf"}
    )
    assert chunks == [] and warnings == []
    assert errors == [{"message": "An error occurred while processing the document."}]
    assert CANARY not in caplog.text


@pytest.mark.parametrize("failure", [asyncio.CancelledError(CANARY), KeyboardInterrupt(CANARY)])
def test_document_cancellation_is_not_suppressed_by_finally(modules, failure):
    modules.factory.ChunkerFactory.return_value.get_chunker.side_effect = failure
    with pytest.raises(type(failure)) as caught:
        modules.document.DocumentChunker().chunk_documents({"fileName": "document.pdf"})
    assert caught.value is failure


def test_document_metadata_failure_is_not_replaced_by_unbound_local(modules):
    with pytest.raises(KeyError, match="documentUrl"):
        modules.document.DocumentChunker().chunk_documents({})


def test_document_success_preserves_chunk_identity_and_order(modules):
    chunks = [{"chunk_id": 1, "content": "first"}, {"chunk_id": 2, "content": "second"}]
    modules.factory.ChunkerFactory.return_value.get_chunker.return_value.get_chunks.return_value = chunks
    result = modules.document.DocumentChunker().chunk_documents({"fileName": "document.pdf"})
    assert result == (chunks, [], [])
    assert result[0] is chunks


def test_title_expected_bad_input_falls_back_but_unexpected_defect_propagates(
    modules, monkeypatch, caplog,
):
    boundary = object.__new__(modules.base.BaseChunker)
    assert boundary._extract_title_from_filename("annualReport-2026.pdf") == "Annual Report 2026"
    assert boundary._extract_title_from_filename(42) == "filename"
    failure = RuntimeError(CANARY)
    monkeypatch.setattr(
        modules.base, "os",
        types.SimpleNamespace(path=types.SimpleNamespace(splitext=Mock(side_effect=failure))),
    )
    with pytest.raises(RuntimeError) as caught:
        boundary._extract_title_from_filename("document.pdf")
    assert caught.value is failure
    assert CANARY not in caplog.text


@pytest.mark.parametrize("split", [False, True])
@pytest.mark.parametrize("expected", [False, True])
def test_analysis_retries_only_service_failures_and_preserves_identity(
    modules, tmp_path, caplog, split, expected,
):
    boundary = analysis_boundary(modules)
    failure = requests.Timeout(CANARY) if expected else RuntimeError(CANARY)
    operation = boundary._analysis_client.analyze_document_from_bytes
    operation.side_effect = failure
    source = tmp_path / "source.pdf"
    source.write_bytes(pdf_bytes())
    with pytest.raises(type(failure)) as caught:
        if split:
            boundary._analyze_split_pdf(str(source))
        else:
            boundary._analyze_single_document()
    assert caught.value is failure
    assert operation.call_count == (3 if expected else 1)
    assert source.exists()
    assert CANARY not in caplog.text


@pytest.mark.parametrize("failure", [requests.Timeout(CANARY), AzureError(CANARY)])
def test_analysis_transient_failure_can_recover_without_changing_result(modules, caplog, failure):
    boundary = analysis_boundary(modules)
    result = ({"content": "document content"}, ["explicit analysis warning"])
    operation = boundary._analysis_client.analyze_document_from_bytes
    operation.side_effect = [failure, result]
    actual = boundary._analyze_single_document()
    assert actual == result and actual[0] is result[0] and actual[1] is result[1]
    assert operation.call_count == 2
    assert CANARY not in caplog.text


def test_pdf_count_programming_failure_does_not_bypass_page_limit(modules, monkeypatch):
    boundary = analysis_boundary(modules)
    failure = RuntimeError(CANARY)
    monkeypatch.setattr(modules.files, "get_pdf_page_count", Mock(side_effect=failure))
    boundary._analyze_single_document = Mock()
    with pytest.raises(RuntimeError) as caught:
        boundary._analyze_document_with_retry()
    assert caught.value is failure
    boundary._analyze_single_document.assert_not_called()


def test_malformed_pdf_count_keeps_logged_remote_analysis_fallback(modules, caplog):
    boundary = analysis_boundary(modules)
    boundary.document_bytes = b"not a PDF"
    result = ({"content": "remote parsed"}, None)
    boundary._analysis_client.analyze_document_from_bytes.return_value = result
    assert boundary._analyze_document_with_retry() == result
    assert "page count" in caplog.text.lower()


@pytest.mark.parametrize("split", [False, True])
@pytest.mark.parametrize("retries", [0, -1])
def test_invalid_retry_budget_does_not_report_empty_success(modules, split, retries):
    boundary = analysis_boundary(modules)
    with pytest.raises(ValueError, match="at least one"):
        if split:
            boundary._analyze_split_pdf("not-opened.pdf", retries=retries)
        else:
            boundary._analyze_single_document(retries=retries)
    boundary._analysis_client.analyze_document_from_bytes.assert_not_called()


@pytest.mark.parametrize("multimodal", [False, True])
def test_analysis_error_list_stays_a_failure_without_exposing_content(modules, multimodal):
    cls = modules.multimodal.MultimodalChunker if multimodal else modules.analysis.DocAnalysisChunker
    boundary = object.__new__(cls)
    boundary.filename = "document.pdf"
    boundary.extension = "pdf"
    boundary.supported_formats = ["pdf"]
    boundary._analysis_client = Mock()
    boundary._analyze_document_with_retry = Mock(return_value=({}, [CANARY]))
    boundary._process_document_chunks = Mock()
    with pytest.raises(RuntimeError, match="^Document analysis returned errors\\.$"):
        boundary.get_chunks()
    boundary._process_document_chunks.assert_not_called()


@pytest.mark.parametrize("failure", [None, RuntimeError(CANARY)])
def test_split_source_and_parts_are_cleaned_on_success_and_failure(
    modules, monkeypatch, tmp_path, failure,
):
    monkeypatch.setattr(modules.files.tempfile, "tempdir", str(tmp_path))
    boundary = analysis_boundary(modules)
    boundary.document_bytes = pdf_bytes(2)
    operation = boundary._analysis_client.analyze_document_from_bytes
    operation.side_effect = failure or [({"content": "first"}, None), ({"content": "second"}, None)]
    if failure:
        with pytest.raises(RuntimeError) as caught:
            boundary._analyze_document_with_retry()
        assert caught.value is failure
    else:
        document, errors = boundary._analyze_document_with_retry()
        assert document == {"content": "first\n<!-- PageBreak -->\nsecond"}
        assert errors is None and boundary._total_pages_analyzed == 2
        for call in operation.call_args_list:
            assert len(PdfReader(io.BytesIO(call.kwargs["file_bytes"])).pages) == 1
    assert list(tmp_path.iterdir()) == []


def test_chunk_creation_preserves_fields_utf8_bound_and_embedding_input(modules):
    boundary = object.__new__(modules.base.BaseChunker)
    boundary.url = "https://example.test/container/document.pdf"
    boundary.filepath = "folder/document.pdf"
    boundary.filename = "document.pdf"
    boundary.embeddings_vector_size = 2
    boundary.aoai_client = Mock()
    boundary.aoai_client.get_embeddings.return_value = [1.0, 2.0]
    content = "\N{LATIN SMALL LETTER E WITH ACUTE}" * 17000
    chunk = boundary._create_chunk(7, content, page=3, offset=15, related_files=["source"])
    assert chunk["chunk_id"] == 7 and chunk["page"] == 3 and chunk["offset"] == 15
    assert chunk["url"] == boundary.url and chunk["filepath"] == boundary.filepath
    assert chunk["relatedFiles"] == ["source"] and chunk["relatedImages"] == []
    assert chunk["content"].encode("utf-8") == content.encode("utf-8")[:32766]
    assert chunk["contentVector"] == [1.0, 2.0] and chunk["captionVector"] == [0.0, 0.0]
    boundary.aoai_client.get_embeddings.assert_called_once_with(chunk["content"])


@pytest.mark.parametrize("failure", [AzureError(CANARY), RuntimeError(CANARY)])
def test_figure_upload_failure_cannot_return_success_shaped_empty_url(modules, caplog, failure):
    boundary = object.__new__(modules.multimodal.MultimodalChunker)
    boundary.filename = "document.pdf"
    boundary.storage_account_name = "example"
    boundary.image_container = "images"
    client = modules.multimodal.BlobClient.return_value
    upload = client.blob_service_client.get_container_client.return_value.get_blob_client.return_value
    upload.upload_blob.side_effect = failure
    with pytest.raises(type(failure)) as caught:
        boundary._upload_figure_blob(b"image", "figure.png")
    assert caught.value is failure
    assert CANARY not in caplog.text


@pytest.mark.parametrize("expected", [False, True])
def test_caption_only_expected_api_failure_is_optional(modules, caplog, expected):
    boundary = object.__new__(modules.multimodal.MultimodalChunker)
    boundary.filename = "document.pdf"
    failure = (
        openai.APIError(CANARY, request=httpx.Request("POST", "https://example.test"), body=None)
        if expected else RuntimeError(CANARY)
    )
    boundary.aoai_client = Mock()
    boundary.aoai_client.get_completion.side_effect = failure
    if expected:
        assert boundary._generate_caption_for_figure({"id": "1.1", "image": "image"}) == "No caption available."
    else:
        with pytest.raises(RuntimeError) as caught:
            boundary._generate_caption_for_figure({"id": "1.1", "image": "image"})
        assert caught.value is failure
    assert CANARY not in caplog.text


def test_caption_success_does_not_log_document_content(modules, caplog):
    caplog.set_level(logging.DEBUG)
    boundary = object.__new__(modules.multimodal.MultimodalChunker)
    boundary.filename = "document.pdf"
    boundary.aoai_client = Mock()
    boundary.aoai_client.get_completion.return_value = CANARY
    assert boundary._generate_caption_for_figure({"id": "1.1", "image": "image"}) == CANARY
    assert CANARY not in caplog.text


@pytest.mark.parametrize("failure", [None, requests.HTTPError(CANARY), AzureError(CANARY), RuntimeError(CANARY)])
def test_figure_attachment_keeps_confirmed_partial_results_but_not_programming_failures(
    modules, caplog, failure,
):
    boundary = object.__new__(modules.multimodal.MultimodalChunker)
    boundary.filename = "document.pdf"
    boundary.filepath = "folder/document.pdf"
    boundary.image_container = "images"
    boundary._use_format_extraction = False
    boundary._docint_client = Mock()
    boundary._docint_client.get_figure.side_effect = [failure or b"first", b"second"]
    boundary._upload_figure_blob = Mock(side_effect=lambda data, name: f"https://example.test/{name}")
    boundary._generate_caption_for_figure = Mock(return_value="caption")
    boundary.aoai_client = Mock()
    boundary.aoai_client.get_embeddings.return_value = [0.25, 0.75]
    document = {"figures": [{"id": "1.1"}, {"id": "1.2"}], "model_id": "model", "result_id": "result"}
    chunks = [{"chunk_id": 7, "content": "before <figure1.1> between <figure1.2> after"}]
    if type(failure) is RuntimeError:
        with pytest.raises(RuntimeError) as caught:
            boundary._attach_figures_to_chunks(document, chunks)
        assert caught.value is failure
        assert boundary._docint_client.get_figure.call_count == 1
    else:
        boundary._attach_figures_to_chunks(document, chunks)
        expected_ids = ["1.2"] if failure else ["1.1", "1.2"]
        assert chunks[0]["chunk_id"] == 7
        assert chunks[0]["relatedImages"] == [
            f"https://example.test/folder-document.pdf-figure-{identifier}.png" for identifier in expected_ids
        ]
        assert chunks[0]["captionVector"] == [0.25, 0.75]
        assert chunks[0]["content"].startswith("before ") and chunks[0]["content"].endswith(" after")
        assert len(chunks[0]["imageCaptions"].splitlines()) == len(expected_ids)
    assert CANARY not in caplog.text


@pytest.mark.parametrize("name", ["extract_figure_from_pdf", "_extract_all_pdf_images"])
def test_pdf_figure_programming_failure_propagates(modules, monkeypatch, name):
    failure = RuntimeError(CANARY)
    monkeypatch.setattr(fitz, "open", Mock(side_effect=failure))
    args = [b"pdf"]
    if name == "extract_figure_from_pdf":
        args.append({"boundingRegions": [{"pageNumber": 1, "polygon": [0, 0, 1, 1]}]})
    with pytest.raises(RuntimeError) as caught:
        getattr(modules.figures, name)(*args)
    assert caught.value is failure


@pytest.mark.parametrize("name", ["extract_figure_from_pdf", "_extract_all_pdf_images"])
def test_pdf_document_closes_when_rendering_fails(modules, monkeypatch, name):
    content = pdf_bytes()
    document = fitz.open(stream=content, filetype="pdf")
    failure = RuntimeError(CANARY)
    monkeypatch.setattr(fitz, "open", Mock(return_value=document))
    monkeypatch.setattr(fitz.Page, "get_pixmap", Mock(side_effect=failure))
    args = [content]
    if name == "extract_figure_from_pdf":
        args.append({"boundingRegions": [{"pageNumber": 1, "polygon": [0, 0, 1, 1]}]})
    try:
        with pytest.raises(RuntimeError) as caught:
            getattr(modules.figures, name)(*args)
        assert caught.value is failure
        assert document.is_closed
    finally:
        if not document.is_closed:
            document.close()


def test_pdf_figure_malformed_and_valid_format_controls(modules):
    region = {"boundingRegions": [{"pageNumber": 1, "polygon": [0, 0, 1, 1]}]}
    assert modules.figures.extract_figure_from_pdf(b"not a PDF", region) is None
    assert modules.figures._extract_all_pdf_images(b"not a PDF") == []
    cropped = modules.figures.extract_figure_from_pdf(pdf_bytes(), region)
    with Image.open(io.BytesIO(cropped)) as image:
        assert image.format == "PNG" and image.size == (200, 200)
    assert len(modules.figures._extract_all_pdf_images(pdf_bytes(2))) == 2


@pytest.mark.parametrize(("page", "polygon"), [
    (0, [0, 0, 1, 1]), (2, [0, 0, 1, 1]), (1, [0, 0, 1]),
    (1, [0, 0, float("inf"), 1]), (1, [0, 0, "invalid", 1]),
])
def test_invalid_pdf_figure_metadata_is_logged_without_rendering(modules, caplog, page, polygon):
    region = {"boundingRegions": [{"pageNumber": page, "polygon": polygon}]}
    assert modules.figures.extract_figure_from_pdf(pdf_bytes(), region) is None
    assert caplog.records


def test_ooxml_expected_corruption_retains_only_confirmed_images(modules):
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as package:
        package.writestr("word/media/b.png", b"corrupt-me")
        package.writestr("word/media/a.png", b"first")
        package.writestr("word/not-media/c.png", b"unrelated")
    assert modules.figures._extract_images_from_ooxml(archive.getvalue(), "word/media/") == [
        b"first", b"corrupt-me",
    ]
    corrupted = archive.getvalue().replace(b"corrupt-me", b"corrupt-no", 1)
    assert modules.figures._extract_images_from_ooxml(corrupted, "word/media/") == [b"first"]


def test_ooxml_programming_failure_propagates(modules, monkeypatch):
    failure = RuntimeError(CANARY)
    monkeypatch.setattr(zipfile, "ZipFile", Mock(side_effect=failure))
    with pytest.raises(RuntimeError) as caught:
        modules.figures._extract_images_from_ooxml(b"archive", "word/media/")
    assert caught.value is failure


@pytest.mark.parametrize("failure", [RuntimeError(CANARY), KeyboardInterrupt(CANARY)])
def test_temp_writer_failure_cleans_file_and_preserves_original(
    modules, monkeypatch, tmp_path, failure,
):
    monkeypatch.setattr(modules.files.tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(PdfWriter, "write", Mock(side_effect=failure))
    source = tmp_path / "source.pdf"
    # Construct input independently of the writer method under test.
    with fitz.open() as document:
        document.new_page()
        document.new_page()
        document.save(source)
    with pytest.raises(type(failure)) as caught:
        list(modules.files.split_pdf_to_temp_files(str(source), max_pages=1))
    assert caught.value is failure
    assert list(tmp_path.iterdir()) == [source]
