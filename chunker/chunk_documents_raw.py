import os
import re
from .text_chunker import TextChunker
from .chunk_metadata_helper import ChunkEmbeddingHelper
from datetime import datetime, timezone
from utils.file_utils import get_filename
from tools.blob import BlobStorageClient
from uuid import uuid4
import logging

def has_supported_file_extension(file_path: str) -> bool:
    """Checks if the given file format is supported based on its file extension.
    Args:
        file_path (str): The file path of the file whose format needs to be checked.
    Returns:
        bool: True if the format is supported, False otherwise.
    """
    return TextChunker()._get_file_format(file_path) is not None

def chunk_document(data):
    chunks = []
    errors = []
    warnings = []

    num_tokens = int(os.getenv("NUM_TOKENS", "256"))
    min_chunk_size = int(os.getenv("MIN_CHUNK_SIZE", "10"))
    token_overlap = int(os.getenv("TOKEN_OVERLAP", "0"))
    sleep_interval_seconds = int(os.getenv("SLEEP_INTERVAL", "8"))

    # Try to get date_uploaded from blob metadata
    try:
        blob_client = BlobStorageClient(data['documentUrl'])
        metadata = blob_client.get_metadata()
        date_uploaded = metadata.get('date_uploaded')
        
        if not date_uploaded:
            # Fallback to current time if not in metadata
            date_uploaded = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            logging.debug(f"No date_uploaded in metadata for {get_filename(data['documentUrl'])}, using current time")
        else:
            logging.debug(f"Using date_uploaded from metadata for {get_filename(data['documentUrl'])}: {date_uploaded}")
            
    except Exception as e:
        # Fallback to current time if there's an error
        date_uploaded = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        logging.warning(f"Error retrieving date_uploaded from metadata for {get_filename(data['documentUrl'])}: {e}. Using current time.")

    chunking_result = TextChunker().chunk_content(data['documentContent'], file_path=data['documentUrl'].split('/')[-1], num_tokens=num_tokens, min_chunk_size=min_chunk_size, token_overlap=token_overlap)
    content_chunk_metadata = ChunkEmbeddingHelper().generate_chunks_with_embedding(data['documentUrl'], [c.content for c in chunking_result.chunks], 'content', sleep_interval_seconds)

    for document_chunk, embedding_metadata in zip(chunking_result.chunks, content_chunk_metadata):
        document_chunk.embedding_metadata = embedding_metadata    

    for chunk in chunking_result.chunks:

        chunks.append({
            "id": str(uuid4()),
            "filepath": get_filename(data['documentUrl']),
            "chunk_id": chunk.embedding_metadata['index'], # type: ignore
            "offset": chunk.embedding_metadata['offset'],  # type: ignore
            "page": chunk.embedding_metadata['page'],  # type: ignore            
            "length": chunk.embedding_metadata['length'],  # type: ignore
            "title": chunk.title,
            "category": "default",
            "metadata_storage_path": data['documentUrl'],
            "url": data['documentUrl'],
            "metadata_storage_name": get_filename(data['documentUrl']),
            "date_uploaded": date_uploaded,  # Use the date from metadata
            "content": chunk.content,
            "vector": chunk.embedding_metadata['embedding'] # type: ignore
        })

    return chunks, errors, warnings
