import logging
import os
import re

from charset_normalizer import detect
from tools import AzureOpenAIClient, GptTokenEstimator
from utils.file_utils import get_file_extension

class BaseChunker:
    """
    BaseChunker class serves as an abstract base class for implementing chunking strategies
    across various document formats. It provides essential methods for managing and processing
    document content, enabling subclasses to define specific chunking logic.

    Initialization:
    ---------------
    The BaseChunker class is initialized with a `data` dictionary containing the document's metadata
    and content. The dictionary can include the following keys:

    Required Keys:
    --------------
    - `documentUrl` (str): The document's URL.
    - `documentContentType` (str): The MIME type of the document content.

    Optional Keys:
    --------------
    - `documentSasToken` (str): The SAS token for accessing the document. Can be an empty string
      if not using storage account or key-based storage access.
    - `documentContent` (str): The raw content of the document. Defaults to an empty string if not provided.
    - `documentBytes` (bytes): The binary content of the document. If not provided, `document_bytes` is set to `None`,
      and a warning is logged.

    Key Attributes:
    ---------------
    - `url` (str): The document's URL.
    - `sas_token` (str): The SAS token for accessing the document. May be empty if not required.
    - `file_url` (str): The full URL constructed by concatenating `url` and `sas_token`.
    - `filename` (str): The name of the file extracted from the URL.
    - `extension` (str): The file extension extracted from the URL.
    - `document_content` (str): The raw content of the document.
    - `document_bytes` (bytes or None): The binary content of the document if provided; otherwise, `None`.
    - `token_estimator` (GptTokenEstimator): An instance for estimating token counts.
    - `aoai_client` (AzureOpenAIClient): An instance of the Azure OpenAI client initialized with the filename.

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
    - If `document_bytes` is not provided during initialization, a warning is logged to inform
      the user.

    Logging:
    --------
    - The class includes logging for truncation warnings and title extraction errors to facilitate
      debugging and monitoring of the chunking process.
    """

    def __init__(self, data):
        """
        Initializes the BaseChunker with the provided data dictionary.

        Parameters
        ----------
        data : dict
            A dictionary containing the following keys:

            Required:
                - "documentUrl" (str): The URL of the document.
                - "documentContentType" (str): The MIME type of the document content.
                - "documentBytes" (bytes): The binary content of the document.

            Optional:
                - "documentSasToken" (str): The SAS token for accessing the document. Can be an empty string
                  if not using storage account or key-based storage access.
                - "documentContent" (str): The raw content of the document.
        
        Attributes
        ----------
        url : str
            The document's URL.
        sas_token : str
            The SAS token for accessing the document. May be empty if not required.
        file_url : str
            The full URL constructed by concatenating `url` and `sas_token`.
        filename : str
            The name of the file extracted from the URL.
        extension : str
            The file extension extracted from the URL.
        document_content : str
            The raw content of the document.
        document_bytes : bytes or None
            The binary content of the document if provided; otherwise, `None`.
        token_estimator : GptTokenEstimator
            An instance for estimating token counts.
        aoai_client : AzureOpenAIClient
            An instance of the Azure OpenAI client initialized with the filename.
        """
        self.data = data
        self.url = data['documentUrl']
        self.sas_token = data.get('documentSasToken', "")
        self.file_url = f"{self.url}{self.sas_token}"
        self.filename = data['fileName']
        self.extension = get_file_extension(self.url)
        document_content = data.get('documentContent') 
        self.document_content = document_content if document_content else ""
        self.token_estimator = GptTokenEstimator()
        self.aoai_client = AzureOpenAIClient(document_filename=self.filename)
        document_bytes = data.get('documentBytes') 
        if document_bytes:
            self.document_bytes = document_bytes 
        else:
            self.document_bytes = None
            logging.warning(f"[base_chunker][{self.filename}] Document bytes not provided.")
        self.embeddings_vector_size = int(os.getenv("AZURE_EMBEDDINGS_VECTOR_SIZE", "3072"))
        
    def get_chunks(self):
        """Abstract method to be implemented by subclasses."""
        pass

    def _create_chunk(
        self,
        chunk_id,
        content,
        summary="",
        embedding_text="",
        title="",
        page=0,
        offset=0,
        related_images=None,
        related_files=None
    ):
        """
        Initialize a chunk dictionary with truncated content if necessary.

        This method creates a chunk dictionary with various attributes, including an embedding vector.
        If an embedding_text is provided, it will use the embedding_text to generate the embedding.
        If no embedding_text is available, it will fall back to using the content text.

        Args:
            chunk_id (str): Sequential number for the chunk.
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
        # Initialize related_images and related_files if they are None
        if related_images is None:
            related_images = []
        if related_files is None:
            related_files = []

        # Define the maximum allowed byte size for the content field
        MAX_CONTENT_BYTES = 32766

        # Function to truncate content to fit within the byte limit without breaking UTF-8 characters
        def truncate_content(content_str, max_bytes):
            encoded_content = content_str.encode('utf-8')
            if len(encoded_content) <= max_bytes:
                return content_str  # No truncation needed
            # Truncate the byte array to the maximum allowed size
            truncated_bytes = encoded_content[:max_bytes]
            # Decode back to string, ignoring any incomplete characters at the end
            return truncated_bytes.decode('utf-8', 'ignore')

        # Truncate the content if it exceeds the maximum byte size
        truncated_content = truncate_content(content, MAX_CONTENT_BYTES)

        # Optionally, you can log or handle the truncation event here
        # For example:
        # if truncated_content != content:
        #     self.logger.warning(f"Content truncated from {len(content.encode('utf-8'))} to {MAX_CONTENT_BYTES} bytes.")

        # Use summary for embedding if available; otherwise, use truncated content
        embedding_text = embedding_text if embedding_text else truncated_content
        content_vector = self.aoai_client.get_embeddings(embedding_text)

        return {
            "chunk_id": chunk_id,
            "url": self.url,
            "filepath": self.filename,
            "content": truncated_content,
            "imageCaptions": "",
            "summary": summary,
            "category": "",
            "length": len(truncated_content),  # Length in characters
            "contentVector": content_vector,
            "captionVector": [0.0] * self.embeddings_vector_size,
            "title": self._extract_title_from_filename(self.filename) if not title else title,
            "page": page,
            "offset": offset,
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

    def decode_to_utf8(self,blob_data):
        # Detect the encoding
        detected = detect(blob_data)
        encoding = detected.get('encoding', 'utf-8')  # Default to UTF-8 if detection fails
        # Decode the data to text using the detected encoding
        try:
            text = blob_data.decode(encoding, errors='replace')
        except (UnicodeDecodeError, LookupError):
            # Fallback in case of errors
            logging.info(f"[base_chunker][{self.filename}] Failed to decode with detected encoding: {encoding}. Falling back to 'utf-8'.")
            text = blob_data.decode('utf-8', errors='replace')
        
        return text