from .base_chunker import BaseChunker
from tools import DocumentIntelligenceClient
from ..exceptions import UnsupportedFormatError
import logging
import os
import re
from langchain.text_splitter import MarkdownTextSplitter, RecursiveCharacterTextSplitter


class DocAnalysisChunker(BaseChunker):
    """
    DocAnalysisChunker class is responsible for analyzing and splitting document content into chunks
    based on specific format criteria, utilizing the Document Intelligence service for analysis.
    
    Format Support:
    ---------------
    The DocAnalysisChunker uses the Document Intelligence service to support a wide range of formats
    and ensure the document's content is accurately analyzed and chunked:

    - Supported Formats: The chunker supports formats that the Document Intelligence client can analyze.
    - Unsupported Formats: Raises an `UnsupportedFormatError` if the document format is not supported by the client.

    Chunking Parameters:
    --------------------
    - max_chunk_size: The maximum size of each chunk in terms of tokens. This is set from the environment variable `NUM_TOKENS` (default is 2048).
    - token_overlap: The number of overlapping tokens between consecutive chunks. This is set from the environment variable `TOKEN_OVERLAP` (default is 100).
    - minimum_chunk_size: The minimum size of each chunk in terms of tokens. This is set from the environment variable `MIN_CHUNK_SIZE` (default is 100).

    Document Analysis:
    ------------------
    - Analyzes the document using the Document Intelligence service and handles the extraction of content.
    - The analysis process identifies the number of pages and provides a sneak peek of the content.
    - Logs warnings if the document is large, suggesting potential timeout issues during processing.
    
    Content Chunking:
    -----------------
    - Splits the document content into manageable chunks using format-specific splitting strategies.
    - Handles specific content structures like HTML tables by replacing them with placeholders during chunking.
    - Supports restoring original content, such as HTML tables, after chunking.
    - Includes a mechanism to manage and update page numbers based on the presence of page breaks.
    
    Error Handling:
    ---------------
    - Comprehensive error handling during document analysis, including specific errors for unsupported formats and general exceptions.
    - Logs the progress and outcomes of the chunking process, including the number of chunks created or skipped.
    """

    def __init__(self, data, max_chunk_size=None, minimum_chunk_size=None, token_overlap=None):
        super().__init__(data)
        self.max_chunk_size = max_chunk_size or int(os.getenv("NUM_TOKENS", "2048"))
        self.minimum_chunk_size = minimum_chunk_size or int(os.getenv("MIN_CHUNK_SIZE", "100"))
        self.token_overlap = token_overlap or int(os.getenv("TOKEN_OVERLAP", "100"))
        self.docint_client = DocumentIntelligenceClient()
        self.supported_formats = self.docint_client.file_extensions

    def get_chunks(self):
        """
        Splits the document content into chunks based on the specified format and criteria.
    
        Returns:
            list: A list of dictionaries, each representing a chunk of the document.
        """
        chunks = []
        if self.extension not in self.supported_formats:
            raise UnsupportedFormatError(f"[doc_analysis_chunker] {self.extension} format is not supported")

        
        logging.info(f"[doc_analysis_chunker] Analyzing {self.filename}.")
        
        document, analysis_errors = self._analyze_document_with_retry()
        if analysis_errors:
            formatted_errors = ', '.join(map(str, analysis_errors))
            raise Exception(f"[doc_analysis_chunker] Error analyzing {self.filename}: {formatted_errors}")

        chunks = self._process_document_chunks(document)

        return chunks

    def _analyze_document_with_retry(self, retries=3):
        """
        Analyzes the document using Document Intelligence Client with a retry mechanism.
        
        Args:
            retries (int): The number of retries allowed.
        
        Returns:
            tuple: A tuple containing the document analysis results and any errors.
        """
        for attempt in range(retries):
            try:
                document, analysis_errors = self.docint_client.analyze_document(self.file_url)
                return document, analysis_errors
            except Exception as e:
                logging.error(f"[doc_analysis_chunker] Analysis failed on attempt {attempt + 1}/{retries}: {str(e)}")
                if attempt == retries - 1:
                    raise
        return None, None

    def _process_document_chunks(self, document):
        """
        Processes the document content into chunks.
        
        Args:
            document (dict): The analyzed document content.
        
        Returns:
            list: A list of processed chunks.
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
                chunk_id += 1
                chunk = self._create_chunk(chunk_id=chunk_id, content=text_chunk, page=chunk_page)
                chunks.append(chunk)
            else:
                skipped_chunks += 1

        logging.info(f"[doc_analysis_chunker] {len(chunks)} chunk(s) created")
        if skipped_chunks > 0:
            logging.info(f"[doc_analysis_chunker] {skipped_chunks} chunk(s) skipped")
        return chunks

    def _chunk_content(self, content):
        """
        Splits the document content into chunks based on the specified format and criteria.
        
        Yields:
            tuple: A tuple containing the chunked content and the number of tokens in the chunk.
        """
        content, placeholders, tables = self._replace_html_tables(content)
        splitter = self._choose_splitter()

        chunks = splitter.split_text(content)

        chunks = self._restore_original_tables(chunks, placeholders, tables)

        for chunked_content in chunks:
            chunk_size = self.token_estimator.estimate_tokens(chunked_content)
            if chunk_size > self.max_chunk_size:
                logging.warning(f"[doc_analysis_chunker] Truncating {chunk_size} size chunk to fit within {self.max_chunk_size} tokens")
                chunked_content = self._truncate_chunk(chunked_content)

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
        if self.docint_client.output_content_format == "markdown":
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
        # Find all PageBreak elements in the content
        matches = re.findall(r'PageBreak(\d{5})', content)
        if matches:
            # Get the last page number
            page_number = int(matches[-1])
            # If the page number is greater than the current page, update it
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
            logging.warning("Token limit reached, truncating...")
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
            page_break_text = " <!-- " + page_break + " -->"
            if page_break not in text:
                # Calculate the size needed for the page break addition
                needed_size = self.token_estimator.estimate_tokens(page_break_text)

                # Truncate exactly the size needed to accommodate the page break
                while self.token_estimator.estimate_tokens(text) + needed_size > self.max_chunk_size:
                    text = text[:-1]  # Remove one character at a time

                # Now add the page break
                text += page_break_text

        return text
