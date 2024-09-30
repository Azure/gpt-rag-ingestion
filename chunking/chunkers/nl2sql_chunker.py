import logging
import os
import json

from .base_chunker import BaseChunker

class NL2SQLChunker(BaseChunker):
    """
    NL2SQLChunker is a class designed to process and chunk JSON content that contains natural language questions and corresponding SQL queries. It reads the JSON data, extracts relevant fields, and creates chunks suitable for embedding or further processing.

    Initialization:
    ---------------
    The NL2SQLChunker is initialized with the following parameters:
    - data (str): The JSON content to be chunked.
    - max_chunk_size (int, optional): The maximum size of each chunk in tokens. Defaults to 2048 tokens or the value specified in the `NUM_TOKENS` environment variable.
    - token_overlap (int, optional): The number of overlapping tokens between consecutive chunks. Defaults to 100 tokens.

    Methods:
    --------
    - get_chunks():
        Processes the JSON content and generates chunks based on the specified chunking parameters. Each 'consulta' in the JSON is treated as a separate chunk. The method includes token size estimation and handles cases where the chunk size exceeds the maximum allowed tokens.

    Attributes:
    -----------
    - max_chunk_size (int): Maximum allowed tokens per chunk.
    - token_overlap (int): Number of overlapping tokens between chunks.
    - token_estimator: A utility for estimating the number of tokens in a given text.
    """

    def __init__(self, data, max_chunk_size=None, token_overlap=None):
        """
        Initializes the NL2SQLChunker with the given data and sets up chunking parameters from environment variables.
        
        Args:
            data (str): The JSON content to be chunked.
        """
        super().__init__(data)
        self.max_chunk_size = max_chunk_size or int(os.getenv("NUM_TOKENS", "2048"))
        self.token_overlap = token_overlap or 100

    def get_chunks(self):
        chunks = []
        logging.info(f"[nl2sql_chunker][{self.filename}] Running get_chunks.")

        blob_data = self.blob_client.download_blob()
        # Decode the bytes into text (assuming it's UTF-8 encoded)
        text = blob_data.decode('utf-8')

        # Parse the JSON data
        try:
            json_data = json.loads(text)
            logging.info(f"[nl2sql_chunker][{self.filename}] Successfully parsed JSON data.")
        except json.JSONDecodeError as e:
            logging.error(f"[nl2sql_chunker][{self.filename}] Failed to parse JSON data: {e}")
            return chunks

        chunk_id = 0
        for query_id, data in json_data.items():
            chunk_id += 1
            content = json.dumps(data, indent=4, ensure_ascii=False)
            chunk_size = self.token_estimator.estimate_tokens(content)
            if chunk_size > self.max_chunk_size:
                logging.warning(f"[nl2sql_chunker][{self.filename}] Chunk {chunk_id} size {chunk_size} exceeds max_chunk_size {self.max_chunk_size}.")
                # Since each chunk corresponds to a single 'query', truncation might not be feasible without data loss.
                # Proceeding with the chunk as is.
            embedding_text = data.get("question", "")
            chunk_dict = self._create_chunk(
                chunk_id=chunk_id,
                content=content,
                embedding_text=embedding_text,
                summary=None
            )
            chunks.append(chunk_dict)

        return chunks
