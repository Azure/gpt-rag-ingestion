import logging
import time
import jsonschema

from utils import get_filename_from_data
from .chunker_factory import ChunkerFactory

class DocumentChunker:
    """
    DocumentChunker class is responsible for processing and chunking documents into smaller parts.
    
    Chunking Process:
    -----------------
    The DocumentChunker handles the entire process of chunking a document, from initializing the appropriate
    chunker based on the document's file extension to splitting the document into manageable chunks.

    - Extension-based Chunking: The class uses `ChunkerFactory` to determine the correct chunker based on the file extension.
    - Error Handling: The class includes mechanisms to handle and log general errors.

    Error Messages:
    ---------------
    - Generates specific error messages for different scenarios.
    - Logs errors and exceptions with detailed information for debugging purposes.

    Logging:
    --------
    - Logs the chunking process, including the time taken, the number of chunks created, and any errors or warnings encountered.

    Returns:
    --------
    The `chunk` method returns a tuple containing:
    - chunks: The list of document chunks created during the process.
    - errors: A list of error messages encountered during the chunking process.
    - warnings: A list of warnings generated during the chunking process.
    """    
    def __init__(self):
        pass

    def _error_message(self, exception=None, filename=""):
        """Generate an error message based on the error type."""
        error_message = "An error occurred while processing the document."
        if exception is not None:
            error_message += f" Exception: {str(exception)}"

        logging.error(f"[document_chunking]{f'[{filename}]' if filename else ''} Error: {error_message}, Ingested Document: {f'[{filename}]' if filename else ''}")

        return error_message

    def chunk_document(self, data):
        """Chunk the document into smaller parts."""
        chunks = []
        errors = []
        warnings = []

        filename = get_filename_from_data(data)
        try:
            chunker = ChunkerFactory().get_chunker(data)
            chunks = chunker.get_chunks()
        except Exception as e:
            errors.append(self._error_message(exception=e, filename=filename))

        return chunks, errors, warnings

    def _format_messages(self, messages):
        formatted = [{"message": msg} for msg in messages]
        return formatted

    def chunk_documents(self, data):
        """
        Processes and chunks the document provided in the input data, returning the chunks along with any errors or warnings encountered.

        Args:
            data (dict): 
                A dictionary containing the document's metadata and content. Expected keys include:
                - "documentUrl" (str): URL of the document.
                - "documentBytes" (str): Base64-encoded bytes of the document.
                - Additional optional fields as defined in the input schema.

        Returns:
            tuple: 
                A tuple containing three lists:
                - chunks (list[dict]): The list of document chunks created during the process.
                - errors (list[str]): A list of error messages encountered during chunking.
                - warnings (list[str]): A list of warning messages generated during chunking.

        Raises:
            jsonschema.exceptions.ValidationError: If the input data does not conform to the expected schema.
            Exception: For any unexpected errors during the chunking process.

        Example:
            >>> chunker = DocumentChunker()
            >>> chunks, errors, warnings = chunker.chunk_documents(data)
        """
        
        chunks = []
        errors = []
        warnings = []
        
        try:
            start_time = time.time()

            filename = get_filename_from_data(data)

            logging.info(f"[document_chunking][{filename}] chunking document.")

            chunks, errors, warnings = DocumentChunker().chunk_document(data)

        except jsonschema.exceptions.ValidationError as e:
            error_message = f"Invalid request: {e}"
            logging.error(f"[document_chunking] {error_message}")
            errors.append(error_message)

        finally:

            if warnings:
                warnings = self._format_messages(warnings)

            if errors:
                errors = self._format_messages(errors)            
            
            elapsed_time = time.time() - start_time
            
            logging.info(
                f"[document_chunking][{filename}] Finished chunking in {elapsed_time:.2f} seconds. "
                f"{len(chunks)} chunks. {len(errors)} errors. {len(warnings)} warnings."
            )            
            return chunks, errors, warnings