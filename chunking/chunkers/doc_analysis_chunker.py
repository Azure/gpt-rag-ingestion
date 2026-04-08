import logging
import os
import re
import time

from langchain_text_splitters import MarkdownTextSplitter, RecursiveCharacterTextSplitter
from .base_chunker import BaseChunker
from ..exceptions import UnsupportedFormatError
from tools import DocumentIntelligenceClient, ContentUnderstandingClient
from dependencies import get_config
from utils.file_utils import (
    split_pdf_to_temp_files,
    renumber_page_markers,
    save_bytes_to_temp_file,
    _safe_delete,
)

app_config_client = get_config()

class DocAnalysisChunker(BaseChunker):
    """
    DocAnalysisChunker class is responsible for analyzing and splitting document content into chunks
    based on specific format criteria, utilizing the Document Intelligence service for content analysis.

    Format Support:
    ---------------
    The DocAnalysisChunker class leverages the Document Intelligence service to process and analyze
    a wide range of document formats. The class ensures that document content is accurately processed
    and divided into manageable chunks.

    - Supported Formats: The chunker processes document formats supported by the Document Intelligence client.
    - Unsupported Formats: If a document's format is not supported by the client, an `UnsupportedFormatError` is raised.

    Chunking Parameters:
    --------------------
    - max_chunk_size: The maximum size of each chunk in tokens. This value is sourced from the `CHUNKING_NUM_TOKENS` 
    environment variable, with a default of 2048 tokens.
    - token_overlap: The number of overlapping tokens between consecutive chunks, sourced from the `TOKEN_OVERLAP` 
    environment variable, with a default of 100 tokens.
    - minimum_chunk_size: The minimum size of each chunk in tokens, sourced from the `CHUNKING_MIN_CHUNK_SIZE` environment 
    variable, with a default of 100 tokens.

    Document Analysis:
    ------------------
    - The document is analyzed using the Document Intelligence service, extracting its content and structure.
    - The analysis process includes identifying the number of pages and providing a preview of the content.
    - If the document is large, a warning is logged to indicate potential timeout issues during processing.

    Content Chunking:
    -----------------
    - The document content is split into chunks using format-specific strategies.
    - HTML tables in the content are replaced with placeholders during the chunking process to simplify splitting.
    - After chunking, the original content, such as HTML tables, is restored in place of the placeholders.
    - The chunking process also manages page numbering based on the presence of page breaks, ensuring each chunk 
    is correctly associated with its corresponding page.

    Error Handling:
    ---------------
    - The class includes comprehensive error handling during document analysis, such as managing unsupported formats 
    and handling general exceptions.
    - The chunking process's progress and outcomes, including the number of chunks created or skipped, are logged.
    """
    def __init__(self, data, max_chunk_size=None, minimum_chunk_size=None, token_overlap=None):
        super().__init__(data)       
        self.max_chunk_size = max_chunk_size or int(app_config_client.get("CHUNKING_NUM_TOKENS", 2048))
        self.minimum_chunk_size = minimum_chunk_size or int(app_config_client.get("CHUNKING_MIN_CHUNK_SIZE", 100))
        self.token_overlap = token_overlap or int(app_config_client.get("TOKEN_OVERLAP", 200))

        # Use Content Understanding by default; fall back to Document Intelligence
        # when USE_DOCUMENT_INTELLIGENCE is explicitly set to true.
        use_docint = app_config_client.get(
            "USE_DOCUMENT_INTELLIGENCE", "false"
        ).lower() in ("true", "1", "yes")

        if use_docint:
            logging.info(f"[doc_analysis_chunker][{self.filename}] Using DocumentIntelligenceClient (USE_DOCUMENT_INTELLIGENCE=true)")
            self._analysis_client = DocumentIntelligenceClient()
        else:
            logging.info(f"[doc_analysis_chunker][{self.filename}] Using ContentUnderstandingClient (default)")
            self._analysis_client = ContentUnderstandingClient()

        self.supported_formats = self._analysis_client.file_extensions
        # Expose output_content_format for splitter selection
        self.output_content_format = getattr(self._analysis_client, "output_content_format", "markdown")
        self.max_pages_per_analysis = int(app_config_client.get("MAX_PAGES_PER_ANALYSIS", 300))

    def get_chunks(self):
        """
        Analyzes the document and generates content chunks based on the analysis.

        This method performs the following steps:
        1. Checks if the document format is supported.
        2. Analyzes the document using the Document Intelligence Client with a retry mechanism.
        3. Processes the document content into chunks based on the analysis.

        Returns:
            list: A list of dictionaries, each representing a chunk of the document content.

        Raises:
            UnsupportedFormatError: If the document format is not supported.
            Exception: If there is an error during document analysis.
        """
        if self.extension not in self.supported_formats:
            raise UnsupportedFormatError(f"[doc_analysis_chunker] {self.extension} format is not supported")

        logging.info(f"[doc_analysis_chunker][{self.filename}] Running get_chunks.")

        _t0 = time.monotonic()
        document, analysis_errors = self._analyze_document_with_retry()
        self._analysis_elapsed_sec = round(time.monotonic() - _t0, 2)

        # Track total pages analyzed (from page-break markers in markdown)
        # For auto-split PDFs, _total_pages_analyzed is already set inside _analyze_document_with_retry
        if not getattr(self, '_total_pages_analyzed', 0):
            page_breaks = document.get("content", "").count("<!-- PageBreak -->") if document else 0
            self._total_pages_analyzed = page_breaks + 1 if document and document.get("content") else 0
        # Track which analysis service was used
        self._analysis_service = "document_intelligence" if isinstance(self._analysis_client, DocumentIntelligenceClient) else "content_understanding"

        if analysis_errors:
            formatted_errors = ', '.join(map(str, analysis_errors))
            raise Exception(f"Error in doc_analysis_chunker analyzing {self.filename}: {formatted_errors}")

        chunks = self._process_document_chunks(document)
        
        return chunks

    def _analyze_document_with_retry(self, retries=3):
        """
        Analyzes the document using the analysis client, with a retry mechanism.

        For PDF files that exceed ``max_pages_per_analysis`` pages, the document
        is automatically split into smaller parts (temp files on disk) and each
        part is analyzed separately.  The markdown results are concatenated with
        page-break offsets so that downstream page-numbering remains correct.

        Args:
            retries (int): The number of times to retry in case of failure.

        Returns:
            tuple: (document_dict, errors_list)
        """
        # --- Decide whether we need to split ---
        temp_source: str | None = None
        need_split = False

        if self.extension == "pdf":
            # Prefer temp file path from the indexer (already on disk).
            temp_source = self.data.get("documentTempFile")

            if not temp_source and self.document_bytes:
                # Count pages to decide if splitting is needed.
                from utils.file_utils import get_pdf_page_count
                try:
                    page_count = get_pdf_page_count(self.document_bytes)
                except Exception:
                    page_count = 0

                if page_count > self.max_pages_per_analysis:
                    # Write to disk so split_pdf_to_temp_files can work from file.
                    temp_source = save_bytes_to_temp_file(self.document_bytes, suffix=".pdf")
                    need_split = True
            elif temp_source:
                # Already on disk — check page count from file.
                from pypdf import PdfReader
                try:
                    page_count = len(PdfReader(temp_source).pages)
                except Exception:
                    page_count = 0
                need_split = page_count > self.max_pages_per_analysis

        if need_split and temp_source:
            return self._analyze_split_pdf(temp_source, retries)

        # --- Standard single-document analysis ---
        return self._analyze_single_document(retries)

    def _analyze_single_document(self, retries=3):
        """Analyze a single (non-split) document with retries."""
        file_bytes = self.document_bytes
        if not file_bytes:
            # Fall back to reading from temp file if bytes not in memory.
            temp_path = self.data.get("documentTempFile")
            if temp_path:
                with open(temp_path, "rb") as f:
                    file_bytes = f.read()

        for attempt in range(retries):
            try:
                document, analysis_errors = self._analysis_client.analyze_document_from_bytes(
                    file_bytes=file_bytes, filename=self.filename
                )
                return document, analysis_errors
            except Exception as e:
                logging.error(
                    f"[doc_analysis_chunker][{self.filename}] analyze document failed "
                    f"on attempt {attempt + 1}/{retries}: {str(e)}"
                )
                if attempt == retries - 1:
                    raise
        return None, None

    def _analyze_split_pdf(self, source_path: str, retries=3):
        """Split a large PDF into parts, analyze each, and concatenate results."""
        combined_content_parts: list[str] = []
        all_errors: list = []
        page_offset = 0
        part_temp_files: list[str] = []

        try:
            for part_path in split_pdf_to_temp_files(source_path, self.max_pages_per_analysis):
                is_original = (part_path == source_path)
                if not is_original:
                    part_temp_files.append(part_path)

                # Read part bytes into memory (one part at a time — bounded size).
                with open(part_path, "rb") as f:
                    part_bytes = f.read()

                # Analyze this part with retries.
                doc = None
                errors = None
                for attempt in range(retries):
                    try:
                        doc, errors = self._analysis_client.analyze_document_from_bytes(
                            file_bytes=part_bytes, filename=self.filename
                        )
                        break
                    except Exception as e:
                        logging.error(
                            f"[doc_analysis_chunker][{self.filename}] analyze part "
                            f"(offset {page_offset}) failed on attempt "
                            f"{attempt + 1}/{retries}: {str(e)}"
                        )
                        if attempt == retries - 1:
                            raise

                # Free part bytes immediately.
                del part_bytes

                if errors:
                    all_errors.extend(errors)

                if doc and doc.get("content"):
                    markdown = doc["content"]
                    # Offset page markers so downstream numbering is absolute.
                    markdown = renumber_page_markers(markdown, page_offset)
                    combined_content_parts.append(markdown)

                    # Count actual pages in this part for next offset.
                    page_breaks = markdown.count("<!-- PageBreak -->")
                    page_offset += page_breaks + 1  # pages = breaks + 1

                # Delete part temp file right away to free disk.
                if not is_original:
                    _safe_delete(part_path)
                    part_temp_files = [p for p in part_temp_files if p != part_path]

        finally:
            # Ensure all remaining part temp files are cleaned up.
            for p in part_temp_files:
                _safe_delete(p)

        combined_content = "\n".join(combined_content_parts)
        combined_document = {"content": combined_content}

        # Store actual page count before synthetic markers inflate the content
        self._total_pages_analyzed = page_offset

        logging.info(
            f"[doc_analysis_chunker][{self.filename}] Split analysis complete: "
            f"{len(combined_content_parts)} parts, "
            f"{len(combined_content)} chars total"
        )

        return combined_document, all_errors if all_errors else None

    def _process_document_chunks(self, document):
        """
        Processes the analyzed document content into manageable chunks.

        Args:
            document (dict): The analyzed document content provided by the Document Intelligence Client.

        Returns:
            list: A list of dictionaries, where each dictionary represents a processed chunk of the document content.

        The method performs the following steps:
        1. Prepares the document content for chunking, including numbering page breaks.
        2. Splits the content into chunks using a chosen splitting strategy.
        3. Iterates through the chunks, determining their page numbers and creating chunk representations.
        4. Skips chunks that do not meet the minimum size requirement.
        5. Logs the number of chunks created and skipped.
        """
        chunks = []
        document_content = document['content']
        document_content = self._number_pagebreaks(document_content)

        text_chunks = self._chunk_content(document_content)
        chunk_id = 0
        skipped_chunks = 0
        current_page = 1

        for text_chunk, num_tokens in text_chunks:
            current_page = self._update_page(text_chunk, current_page)
            chunk_page = self._determine_chunk_page(text_chunk, current_page)
            if num_tokens >= self.minimum_chunk_size:
                chunk = self._create_chunk(
                    chunk_id=chunk_id,
                    content=text_chunk,
                    page=chunk_page
                )
                chunks.append(chunk)
                chunk_id += 1
            else:
                skipped_chunks += 1

        logging.debug(f"[doc_analysis_chunker][{self.filename}] {len(chunks)} chunk(s) created")
        if skipped_chunks > 0:
            logging.debug(f"[doc_analysis_chunker][{self.filename}] {skipped_chunks} chunk(s) skipped")
        return chunks

    def _chunk_content(self, content):
        """
        Splits the document content into chunks based on the specified format and criteria.
        
        Yields:
            tuple: A tuple containing the chunked content and the number of tokens in the chunk.
        """
        splitter = self._choose_splitter()

        chunks = splitter.split_text(content)

        for chunked_content in chunks:
            chunk_size = self.token_estimator.estimate_tokens(chunked_content)
            yield chunked_content, chunk_size

    def _replace_html_tables(self, content):
        """
        Replaces HTML tables in the content with placeholders.
        
        Args:
            content (str): The document content.
        
        Returns:
            tuple: The content with placeholders and a list of the original tables.
        """
        table_pattern = r"(<table[\s\S]*?</table>)"
        tables = re.findall(table_pattern, content, re.IGNORECASE)
        placeholders = [f"__TABLE_{i}__" for i in range(len(tables))]
        for placeholder, table in zip(placeholders, tables):
            content = content.replace(table, placeholder)
        return content, placeholders, tables

    def _restore_original_tables(self, chunks, placeholders, tables):
        """
        Restores original tables in the chunks from placeholders.
        
        Args:
            chunks (list): The list of text chunks.
            placeholders (list): The list of table placeholders.
            tables (list): The list of original tables.
        
        Returns:
            list: The list of chunks with original tables restored.
        """
        for placeholder, table in zip(placeholders, tables):
            chunks = [chunk.replace(placeholder, table) for chunk in chunks]
        return chunks

    def _choose_splitter(self):
        """
        Chooses the appropriate splitter based on document format.
        
        Returns:
            object: The splitter to use for chunking.
        """
        if self.output_content_format == "markdown":
            return MarkdownTextSplitter.from_tiktoken_encoder(
                chunk_size=self.max_chunk_size,
                chunk_overlap=self.token_overlap
            )
        else:
            separators = [".", "!", "?"] + [" ", "\n", "\t"]
            return RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                separators=separators,
                chunk_size=self.max_chunk_size,
                chunk_overlap=self.token_overlap
            )

    def _number_pagebreaks(self, content):
        """
        Finds and numbers all PageBreaks in the content.
        
        Args:
            content (str): The document content.
        
        Returns:
            str: Content with numbered PageBreaks.
        """
        pagebreaks = re.findall(r'<!-- PageBreak -->', content)
        for i, _ in enumerate(pagebreaks, 1):
            content = content.replace('<!-- PageBreak -->', f'<!-- PageBreak{str(i).zfill(5)} -->', 1)
        return content

    def _update_page(self, content, current_page):
        """
        Updates the current page number based on the content.
        
        Args:
            content (str): The content chunk being processed.
            current_page (int): The current page number.
        
        Returns:
            int: The updated current page number.
        """
        matches = re.findall(r'PageBreak(\d{5})', content)
        if matches:
            page_number = int(matches[-1])
            if page_number >= current_page:
                current_page = page_number + 1
        return current_page

    def _determine_chunk_page(self, content, current_page):
        """
        Determines the chunk page number based on the position of the PageBreak element.
        
        Args:
            content (str): The content chunk being processed.
            current_page (int): The current page number.
        
        Returns:
            int: The page number for the chunk.
        """
        match = re.search(r'PageBreak(\d{5})', content)
        if match:
            page_number = int(match.group(1))
            position = match.start() / len(content)
            # Determine the chunk_page based on the position of the PageBreak element
            if position < 0.5:
                chunk_page = page_number + 1
            else:
                chunk_page = page_number
        else:
            chunk_page = current_page
        return chunk_page

    def _truncate_chunk(self, text):
        """
        Truncates and normalizes the text to ensure it fits within the maximum chunk size.
        
        This method first cleans up the text by removing unnecessary spaces and line breaks. 
        If the text still exceeds the maximum token limit, it iteratively truncates the text 
        until it fits within the limit.

        This method overrides the parent class's method because it includes logic to retain 
        PageBreaks within the truncated text.
        
        Args:
            text (str): The text to be truncated and normalized.
        
        Returns:
            str: The truncated and normalized text.
        """
        # Clean up text (e.g. line breaks)
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'[\n\r]+', ' ', text).strip()

        page_breaks = re.findall(r'PageBreak\d{5}', text)

        # Truncate if necessary
        if self.token_estimator.estimate_tokens(text) > self.max_chunk_size:
            logging.info(f"[doc_analysis_chunker][{self.filename}] token limit reached, truncating...")
            step_size = 1  # Initial step size
            iteration = 0  # Iteration counter

            while self.token_estimator.estimate_tokens(text) > self.max_chunk_size:
                # Truncate the text
                text = text[:-step_size]
                iteration += 1

                # Increase step size exponentially every 5 iterations
                if iteration % 5 == 0:
                    step_size = min(step_size * 2, 100)

        # Reinsert page breaks and recheck size
        for page_break in page_breaks:
            page_break_text = f" <!-- {page_break} -->"
            if page_break not in text:
                # Calculate the size needed for the page break addition
                needed_size = self.token_estimator.estimate_tokens(page_break_text)

                # Truncate exactly the size needed to accommodate the page break
                while self.token_estimator.estimate_tokens(text) + needed_size > self.max_chunk_size:
                    text = text[:-1]  # Remove one character at a time

                # Now add the page break
                text += page_break_text

        return text