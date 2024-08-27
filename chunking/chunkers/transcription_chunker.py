import logging
import webvtt
import os
from .base_chunker import BaseChunker
from io import StringIO
from langchain.text_splitter import RecursiveCharacterTextSplitter

class TranscriptionChunker(BaseChunker):
    def __init__(self, data, max_chunk_size=None, token_overlap=None):
        """
        Initializes the TranscriptionChunker with the given data and sets up chunking parameters from environment variables.
        
        Args:
            data (str): The document content to be chunked.
        """
        super().__init__(data)
        self.max_chunk_size = max_chunk_size or int(os.getenv("NUM_TOKENS", "2048"))
        self.token_overlap = token_overlap or 100

    def get_chunks(self):           
        chunks = [] 
        logging.info(f"[transcription_chunker] Running get_chunks for {self.filename}. Transcription size: {len(self.document_content)} characters")

        # Extract the text from the vtt file
        text = self._vtt_process()
        self.document_content = text
        logging.info(f"[transcription_chunker] Transcription text: {text[:100]}")

        # Get the summary of the text
        prompt = f"Provide clearly elaborated summary along with the keypoints and values mentioned for the transcript of a conversation: {text} "
        summary = self.aoai_client.get_completion(prompt)
        text_chunks = self._chunk_document_content()
        chunk_id = 0
        for text_chunk in text_chunks:
            chunk_id += 1
            chunk_size = self.token_estimator.estimate_tokens(text_chunk)
            if chunk_size > self.max_chunk_size:
                logging.warning(f"[transcription_chunker] Truncating {chunk_size} size chunk to fit within {self.max_chunk_size} tokens")
                text_chunk = self._truncate_chunk(text_chunk)
            chunk_dict = self._create_chunk(chunk_id=chunk_id, content=text_chunk, embedding_text=summary, summary=summary) 
            chunks.append(chunk_dict)      
        return chunks

    def _vtt_process(self):
        vtt = webvtt.read_buffer(StringIO(self.document_content))
        data, text, voice = [], "", ""

        for caption in vtt:
            current_voice = caption.voice or ""
            
            if current_voice != voice:
                if text:
                    data.append(text.replace("\n", " "))
                voice, text = current_voice, f"{voice}: {caption.text} " if voice else caption.text + " "
            else:
                text += caption.text + " "

        if text:
            data.append(text.replace("\n", " "))

        return "\n".join(data).strip()

    def _chunk_document_content(self):

        sentence_endings = [".", "!", "?"]
        word_breaks = [" ", "\n", "\t"]
        splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            separators=sentence_endings + word_breaks,
            chunk_size=self.max_chunk_size, 
            chunk_overlap=self.token_overlap
        )
    
        chunked_content_list = splitter.split_text(self.document_content)
    
        for chunked_content in chunked_content_list:
            yield chunked_content # type: ignore
