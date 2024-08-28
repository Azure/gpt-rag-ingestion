import asyncio
from embedder.text_embedder import TextEmbedder

class ChunkEmbeddingHelper:
    def __init__(self, text_embedder:TextEmbedder):
        self.text_embedder = text_embedder

    @classmethod
    async def create(cls):
        text_embedder = await TextEmbedder.create()
        return cls(text_embedder)


    async def generate_chunks_with_embedding(self, document_id, content_chunks, fieldname, sleep_interval_seconds) ->  dict:
        offset = 0
        page = 0
        chunk_embeddings = []
        for index, (content_chunk) in enumerate(content_chunks):
            metadata = await self._generate_content_metadata(document_id, fieldname, index, content_chunk, offset, page)
            offset += metadata['length']
            chunk_embeddings.append(metadata)
            
            # A very crude way to introduce some delay between each embedding call
            # This is to avoid hitting the rate limit of the OpenAI API
            await asyncio.sleep(sleep_interval_seconds)
        return chunk_embeddings # type: ignore

    async def _generate_content_metadata(self, document_id, fieldname, index, content, offset, page):
        metadata = {'fieldname':fieldname}
        metadata['docid'] = document_id
        metadata['index'] = index
        metadata['offset'] = offset
        metadata['page'] = page        
        metadata['length'] = len(content)
        metadata['embedding'] = await self.text_embedder.embed_content(content)
        return metadata