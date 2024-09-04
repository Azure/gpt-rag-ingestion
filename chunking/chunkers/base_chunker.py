import logging
import os
import re

from tools import AzureOpenAIClient, BlobStorageClient, GptTokenEstimator
from utils.file_utils import get_file_extension, get_filename

class BaseChunker:
    """
    BaseChunker class serves as an abstract base class for implementing chunking strategies
    across various document formats. It provides essential methods for managing and processing
    document content, enabling subclasses to define specific chunking logic.

    Initialization:
    ---------------
    The BaseChunker class is initialized with a data dictionary containing the document's metadata
    and content. Key attributes include:

    - `url`: The document's URL.
    - `document_content`: The raw content of the document.
    - `sas_token`: The SAS token for accessing the document.
    - `filename`: The name of the file extracted from the URL.
    - `extension`: The file extension extracted from the URL.

    Abstract Method:
    ----------------
    - `get_chunks`: An abstract method that must be implemented by subclasses to define
      specific chunking logic. This method is responsible for splitting the document content
      into manageable chunks.

    Chunk Creation:
    ---------------
    - `_create_chunk`: Initializes a chunk dictionary with metadata such as chunk ID, content,
      page number, and related images or files. This method also generates a content vector
      using Azure OpenAI embeddings.

    Title Extraction:
    -----------------
    - `_extract_title_from_filename`: Extracts a title from the document's filename by removing
      the extension, replacing delimiters with spaces, and capitalizing words appropriately.
      This method ensures a user-friendly title is generated for the document.

    Text Truncation and Normalization:
    ----------------------------------
    - `_truncate_and_normalize_text`: Truncates and normalizes the text to ensure it fits
      within a defined maximum chunk size. The method first cleans up unnecessary spaces
      and line breaks and then truncates the text iteratively if it exceeds the token limit.

    Error Handling:
    ---------------
    - Comprehensive error handling is implemented in the `_extract_title_from_filename` method,
      logging any issues encountered during title extraction.
    
    Logging:
    --------
    - The class includes logging for truncation warnings and title extraction errors to facilitate
      debugging and monitoring of the chunking process.
    """    

    def __init__(self, data):
        """
        data : dict
            A dictionary containing the following keys:
                - "documentUrl"
                - "documentSasToken"
                - "documentContentType"
        """
        self.url = data['documentUrl']
        self.data = data
        self.url = data['documentUrl']
        self.sas_token = data['documentSasToken']
        self.file_url = f"{self.url}{self.sas_token}"        
        self.filename = get_filename(self.url)
        self.extension = get_file_extension(self.url)
        document_content = data.get('documentContent') # Reserved for future use: Document content extraction with AI Search is currently not implemented.
        self.document_content = document_content if document_content else ""
        self.token_estimator = GptTokenEstimator()
        self.aoai_client = AzureOpenAIClient(document_filename=self.filename)
        self.blob_client = BlobStorageClient(self.file_url)

    def get_chunks(self):
        """Abstract method to be implemented by subclasses."""
        pass

    def _create_chunk(self, chunk_id, content, summary="", embedding_text="", title="", page=0, offset=0, related_images=[], related_files=[]):
        """
        Initialize a chunk dictionary.

        This method creates a chunk dictionary with various attributes, including an embedding vector. 
        If an embedding_text is provided, it will use the embedding_text to generate the embedding. 
        If no embedding_text is available, it will fall back to using the content text.

        Args:
            chunk_id (str): Unique identifier for the chunk.
            content (str): The main content of the chunk.
            summary (str, optional): A brief summary of the content. Defaults to an empty string.
            embedding_text (str, optional): Text used to generate the embedding. Defaults to an empty string.
            title (str, optional): The title of the chunk. Defaults to an empty string.
            page (int, optional): The page number where the chunk is located. Defaults to 0.
            offset (int, optional): The offset position of the chunk in the content. Defaults to 0.
            related_images (list, optional): A list of related images. Defaults to an empty list.
            related_files (list, optional): A list of related files. Defaults to an empty list.

        Returns:
            dict: A dictionary representing the chunk with all the attributes, including the embedding vector.
        """
        # Use summary for embedding if available; otherwise, use content
        embedding_text = embedding_text if embedding_text else content
        content_vector = self.aoai_client.get_embeddings(embedding_text)
        
        return {
            "chunk_id": chunk_id,
            "url": self.url,
            "filepath": self.filename,
            "content": content,
            "summary": summary,
            "category": "",  
            "length": len(content),                             
            "contentVector": content_vector,
            "title": self._extract_title_from_filename(self.filename) if not title else title,
            "page": page,
            "offset": offset,
            "security_id": [],
            "relatedImages": related_images,
            "relatedFiles": related_files
        }


    
    def _extract_title_from_filename(self, filename):
        """
        Extracts a title from a filename by removing the extension and 
        replacing underscores or other delimiters with spaces, 
        then capitalizing words appropriately.
        
        Args:
            filename (str): The name of the file.
        
        Returns:
            str: The extracted title.
        """
        try:
            # Remove the file extension
            title = os.path.splitext(filename)[0]
    
            # Replace common delimiters with spaces
            title = re.sub(r'[_-]', ' ', title)
    
            # Add a space before any capital letter that follows a lowercase letter or number
            title = re.sub(r'(?<=[a-z0-9])(?=[A-Z])', ' ', title)
    
            # Capitalize the first letter of each word
            title = title.title()
    
            return title
        except Exception as e:
            logging.error(f"[base_chunker][{filename}] Error extracting title from filename '{filename}': {e}")
            return "filename"
        
    def _truncate_chunk(self, text):
        """
        Truncates the chunk to ensure it fits within the maximum chunk size.
        
        This method first cleans up the text by removing unnecessary spaces and line breaks. 
        If the text still exceeds the maximum token limit, it iteratively truncates the text 
        until it fits within the limit.
        
        Args:
            text (str): The text to be truncated.
        
        Returns:
            str: The truncated chunk.
        """
        if self.token_estimator.estimate_tokens(text) > self.max_chunk_size:
            logging.info(f"[base_chunker][{self.filename}] Token limit exceeded maximum length, truncating...")
            step_size = 1  # Initial step size
            iteration = 0  # Iteration counter

            while self.token_estimator.estimate_tokens(text) > self.max_chunk_size:
                text = text[:-step_size]
                iteration += 1

                # Increase step size exponentially every 5 iterations
                if iteration % 5 == 0:
                    step_size = min(step_size * 2, 100)

        return text

                  