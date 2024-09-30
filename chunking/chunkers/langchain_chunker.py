import logging
import os
import re
from .base_chunker import BaseChunker
from ..exceptions import UnsupportedFormatError
from langchain.text_splitter import MarkdownTextSplitter, RecursiveCharacterTextSplitter, PythonCodeTextSplitter

class LangChainChunker(BaseChunker):
    """
    LangChainChunker is a class designed to split document content into chunks based on the format and specific chunking criteria. The class leverages various LangChain splitters tailored for different content formats, ensuring accurate and efficient processing.

    Initialization:
    ---------------
    The LangChainChunker is initialized with the following parameters:
    - data (str): The document content to be chunked.

    Attributes:
    -----------
    - max_chunk_size (int): The maximum allowed size of each chunk in tokens, derived from the `NUM_TOKENS` environment variable (default is 2048 tokens).
    - token_overlap (int): The number of overlapping tokens between consecutive chunks, derived from the `TOKEN_OVERLAP` environment variable (default is 100 tokens).
    - minimum_chunk_size (int): The minimum required size of each chunk in tokens, derived from the `MIN_CHUNK_SIZE` environment variable (default is 100 tokens).
    - supported_formats (dict): A dictionary mapping file extensions to their corresponding content format, used to select the appropriate text splitter.

    Methods:
    --------
    - get_chunks():
        Splits the document content into chunks based on the specified format and criteria. 
        The method first checks if the document's format is supported, then processes the content 
        into chunks, skipping those that don't meet the minimum size requirement. Finally, it logs 
        the number of chunks created and skipped.

    - _chunk_content():
        Splits the document content into chunks according to the format-specific splitting strategy.
        The method identifies the format of the document and chooses the corresponding LangChain splitter 
        (e.g., `MarkdownTextSplitter` for Markdown, `PythonCodeTextSplitter` for Python code, and 
        `RecursiveCharacterTextSplitter` for other formats). It yields each chunk along with its token count.
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
            "json": "json",
            "csv": "csv",
            "xml": "xml"
        }

    def get_chunks(self):
        """
        Splits the document content into chunks based on the specified format and criteria.
        
        Returns:
            list: A list of dictionaries, each representing a chunk of the document.
        """
        chunks = []
    
        if self.extension not in self.supported_formats:
            raise UnsupportedFormatError(f"[langchain_chunker] {self.filename} {self.extension} format is not supported")
        
        # Download the blob as bytes
        blob_data = self.blob_client.download_blob()
        # Decode the bytes into text (assuming it's UTF-8 encoded)
        text = blob_data.decode('utf-8')

        text_chunks = self._chunk_content(text)
        skipped_chunks = 0
        chunk_id = 0
        for text_chunk, num_tokens in text_chunks:
            if num_tokens >= self.minimum_chunk_size:
                chunk_id += 1
                chunk_size = self.token_estimator.estimate_tokens(text_chunk)
                if chunk_size > self.max_chunk_size:
                    logging.info(f"[langchain_chunker][{self.filename}] truncating {chunk_size} size chunk to fit within {self.max_chunk_size} tokens")
                    text_chunk = self._truncate_chunk(text_chunk)
                chunk_dict = self._create_chunk(chunk_id, text_chunk)
                chunks.append(chunk_dict)
            else:
                skipped_chunks += 1
        logging.info(f"[langchain_chunker][{self.filename}] {len(chunks)} chunk(s) created")    
        if skipped_chunks > 0:
            logging.info(f"[langchain_chunker][{self.filename}] {skipped_chunks} chunk(s) skipped")
    
        return chunks
    
    def _chunk_content(self, text):
        """
        Splits the document content into chunks according to the specified format and token limits.

        Args:
            content (str): The full content of the document to be chunked.

        Yields:
            tuple: A tuple containing the chunked content (str) and the number of tokens in the chunk (int).

        The method includes the following steps:
        1. Replaces HTML tables in the content with placeholders to facilitate chunking.
        2. Chooses an appropriate text splitter based on the document's format.
        3. Splits the content into chunks, restoring any original HTML tables after chunking.
        4. Truncates chunks that exceed the maximum token size, ensuring they fit within the limit.
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
    
        chunked_content_list = splitter.split_text(text)
    
        for chunked_content in chunked_content_list:
            chunk_size = self.token_estimator.estimate_tokens(chunked_content)
            yield chunked_content, chunk_size  # type: ignore
