import logging
import os
import re
from .base_chunker import BaseChunker
from ..exceptions import UnsupportedFormatError
from langchain.text_splitter import MarkdownTextSplitter, RecursiveCharacterTextSplitter, PythonCodeTextSplitter

class LangChainChunker(BaseChunker):
    """
    TextChunker class is responsible for splitting document content into chunks based on the specified format and criteria.
    
    Format-specific Splitters:
    -------------------------
    The TextChunker uses different LangChain splitters based on the file format to ensure accurate and efficient chunking:
    
    - Markdown: Uses `MarkdownTextSplitter` to handle markdown-specific chunking.
    - Python: Uses `PythonCodeTextSplitter` to handle Python code-specific chunking.
    - Other Formats: Uses `RecursiveCharacterTextSplitter` with sentence and word separators for other formats like text, HTML, etc.
    
    Chunking Parameters:
    --------------------
    - max_chunk_size: The maximum size of each chunk in terms of tokens. This is set from the environment variable `NUM_TOKENS` (default is 2048).
    - token_overlap: The number of overlapping tokens between consecutive chunks. This is set from the environment variable `TOKEN_OVERLAP` (default is 100).
    - minimum_chunk_size: The minimum size of each chunk in terms of tokens. This is set from the environment variable `MIN_CHUNK_SIZE` (default is 100).
    """

    def __init__(self, data):
        """
        Initializes the TextChunker with the given data and sets up chunking parameters from environment variables.
        
        Args:
            data (str): The document content to be chunked.
        """
        super().__init__(data)
        self.max_chunk_size = int(os.getenv("NUM_TOKENS", "2048"))
        self.minimum_chunk_size = int(os.getenv("MIN_CHUNK_SIZE", "100"))
        self.token_overlap = int(os.getenv("TOKEN_OVERLAP", "100"))
        self.supported_formats = {
            "md": "markdown",
            "txt": "text",
            "html": "html",
            "shtml": "html",
            "htm": "html",
            "py": "python",
            "pdf": "pdf",
            "json": "json",
            "csv": "csv",
            "epub": "epub",
            "rtf": "rtf",
            "xml": "xml",
            "xlsx": "xlsx",
            "xls": "xls",
            "docx": "docx",
            "doc": "doc",
            "pptx": "pptx",
            "ppt": "ppt",
            "msg": "msg"
        }

    def get_chunks(self):
        """
        Splits the document content into chunks based on the specified format and criteria.
        
        Returns:
            list: A list of dictionaries, each representing a chunk of the document.
        """
        chunks = []
    
        if self.extension not in self.supported_formats:
            raise UnsupportedFormatError(f"[langchain_chunker] {self.extension} format is not supported")
        text_chunks = self._chunk_content()
        skipped_chunks = 0
        chunk_id = 0
        for text_chunk, num_tokens in text_chunks:
            if num_tokens >= self.minimum_chunk_size:
                chunk_id += 1
                chunk_size = self.token_estimator.estimate_tokens(text_chunk)
                if chunk_size > self.max_chunk_size:
                    logging.warning(f"[langchain_chunker] Truncating {chunk_size} size chunk to fit within {self.max_chunk_size} tokens")
                    text_chunk = self._truncate_chunk(text_chunk)
                chunk_dict = self._create_chunk(chunk_id, text_chunk)
                chunks.append(chunk_dict)
            else:
                skipped_chunks += 1
        logging.info(f"[langchain_chunker] {len(chunks)} chunk(s) created")    
        if skipped_chunks > 0:
            logging.info(f"[langchain_chunker] {skipped_chunks} chunk(s) skipped")
    
        return chunks
    
    def _chunk_content(self):
        """
        Splits the document content into chunks based on the specified format and criteria.
        
        Yields:
            tuple: A tuple containing the chunked content and the number of tokens in the chunk.
        """
        file_format = self.supported_formats[self.extension]
    
        if file_format == "markdown":
            splitter = MarkdownTextSplitter.from_tiktoken_encoder(
                chunk_size=self.max_chunk_size, 
                chunk_overlap=self.token_overlap
            )
        elif file_format == "python":
            splitter = PythonCodeTextSplitter.from_tiktoken_encoder(
                chunk_size=self.max_chunk_size, 
                chunk_overlap=self.token_overlap
            )
        else:
            sentence_endings = [".", "!", "?"]
            word_breaks = [" ", "\n", "\t"]
            splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
                separators=sentence_endings + word_breaks,
                chunk_size=self.max_chunk_size, 
                chunk_overlap=self.token_overlap
            )
    
        chunked_content_list = splitter.split_text(self.document_content)
    
        for chunked_content in chunked_content_list:
            chunk_size = self.token_estimator.estimate_tokens(chunked_content)
            yield chunked_content, chunk_size  # type: ignore
