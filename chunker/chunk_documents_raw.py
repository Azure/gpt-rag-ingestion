import os
from .text_chunker import TextChunker
from .chunk_metadata_helper import ChunkEmbeddingHelper

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

    num_tokens = int(os.getenv("NUM_TOKENS", "2048"))
    min_chunk_size = int(os.getenv("MIN_CHUNK_SIZE", "100"))
    token_overlap = int(os.getenv("TOKEN_OVERLAP", "100"))
    sleep_interval_seconds = int(os.getenv("SLEEP_INTERVAL", "1"))

    chunking_result = TextChunker().chunk_content(data['documentContent'], file_path=data['documentUrl'].split('/')[-1], num_tokens=num_tokens, min_chunk_size=min_chunk_size, token_overlap=token_overlap)
    content_chunk_metadata = ChunkEmbeddingHelper().generate_chunks_with_embedding(data['documentUrl'], [c.content for c in chunking_result.chunks], 'content', sleep_interval_seconds)

    for document_chunk, embedding_metadata in zip(chunking_result.chunks, content_chunk_metadata):
        document_chunk.embedding_metadata = embedding_metadata    

    for chunk in chunking_result.chunks:
        chunks.append({
            "filepath": data['documentUrl'].split('/')[-1],
            "chunk_id": chunk.embedding_metadata['index'], # type: ignore
            "offset": chunk.embedding_metadata['offset'],  # type: ignore
            "length": chunk.embedding_metadata['length'],  # type: ignore
            "title": chunk.title,
            "category": "default",
            "url": data['documentUrl'],
            "content": chunk.content,
            "contentVector": chunk.embedding_metadata['embedding'] # type: ignore
        })

    # chunks = [{
    #                     "filepath": data['documentUrl'].split('/')[-1],
    #                     "chunk_id": 0,
    #                     "offset": 0,
    #                     "length": 0,
    #                     "title": "default",
    #                     "category": "default",
    #                     "url": data['documentUrl'],
    #                     "content": "AAA",
    #                     "contentVector": [0.1] * 1536,                    
    #                 },
    #                 {
    #                     "filepath": data['documentUrl'].split('/')[-1],
    #                     "chunk_id": 2,
    #                     "offset": 0,
    #                     "length": 0,
    #                     "title": "default",
    #                     "category": "default",
    #                     "url": data['documentUrl'],
    #                     "content": "AAAxxxx",
    #                     "contentVector": [0.1] * 1536,
    # }]

    return chunks, errors, warnings