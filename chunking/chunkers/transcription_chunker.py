import logging
import os
from io import StringIO
from io import BytesIO

import webvtt
from langchain.text_splitter import RecursiveCharacterTextSplitter

from .base_chunker import BaseChunker

class TranscriptionChunker(BaseChunker):
    """
    TranscriptionChunker is a class designed to process and chunk transcription text content, specifically from WebVTT (Web Video Text Tracks) format files. It utilizes the RecursiveCharacterTextSplitter to segment the transcription into manageable chunks, considering token limits and content structure.

    Initialization:
    ---------------
    The TranscriptionChunker is initialized with the following parameters:
    - data (str): The transcription text content to be chunked.
    - max_chunk_size (int, optional): The maximum size of each chunk in tokens. Defaults to 2048 tokens or the value specified in the `NUM_TOKENS` environment variable.
    - token_overlap (int, optional): The number of overlapping tokens between consecutive chunks. Defaults to 100 tokens.

    Methods:
    --------
    - get_chunks():
        Processes the transcription text and generates chunks based on the specified chunking parameters. 
        It first processes the WebVTT file, extracts the text, and then splits the content into chunks. 
        The method includes a mechanism to summarize the content and attaches this summary to each chunk.

    - _vtt_process():
        Converts the WebVTT content into a continuous text block, retaining speaker changes. 
        It processes each caption, merging text from the same speaker and separating segments by speaker changes.

    - _chunk_document_content():
        Splits the processed document content into chunks using the RecursiveCharacterTextSplitter. 
        This method yields each chunk as it is created.

    Attributes:
    -----------
    - max_chunk_size (int): Maximum allowed tokens per chunk.
    - token_overlap (int): Number of overlapping tokens between chunks.
    - document_content (str): The content of the document.
    - aoai_client: An instance for generating summaries and processing content with OpenAI models.
    - token_estimator: A utility for estimating the number of tokens in a given text.
    """

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
        logging.info(f"[transcription_chunker][{self.filename}] Running get_chunks.")

        # Extract the text from the vtt file
        text = self._vtt_process()
        logging.info(f"[transcription_chunker][{self.filename}] transcription text: {text[:100]}")

        # Get the summary of the text
        prompt = f"Provide clearly elaborated summary along with the keypoints and values mentioned for the transcript of a conversation: {text} "
        summary = self.aoai_client.get_completion(prompt)
        text_chunks = self._chunk_document_content(text)
        chunk_id = 0
        for text_chunk in text_chunks:
            chunk_id += 1
            chunk_size = self.token_estimator.estimate_tokens(text_chunk)
            if chunk_size > self.max_chunk_size:
                logging.info(f"[transcription_chunker][{self.filename}] truncating {chunk_size} size chunk to fit within {self.max_chunk_size} tokens")
                text_chunk = self._truncate_chunk(text_chunk)
            chunk_dict = self._create_chunk(chunk_id=chunk_id, content=text_chunk, embedding_text=summary, summary=summary) 
            chunks.append(chunk_dict)      
        return chunks

    def _vtt_process(self):
        blob_data = self.blob_client.download_blob()
        blob_stream = BytesIO(blob_data)
        vtt = webvtt.read_buffer(blob_stream)
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

    def _chunk_document_content(self, text):

        sentence_endings = [".", "!", "?"]
        word_breaks = [" ", "\n", "\t"]
        splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
            separators=sentence_endings + word_breaks,
            chunk_size=self.max_chunk_size, 
            chunk_overlap=self.token_overlap
        )
        chunked_content_list = splitter.split_text(text)
    
        for chunked_content in chunked_content_list:
            yield chunked_content # type: ignore
