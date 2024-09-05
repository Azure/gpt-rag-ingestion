import logging
import time

from utils.file_utils import get_file_extension, get_filename
from .chunker_factory import ChunkerFactory

class DocumentChunker:
    """
    DocumentChunker class is responsible for processing and chunking documents into smaller parts.
    
    Chunking Process:
    -----------------
    The DocumentChunker handles the entire process of chunking a document, from initializing the appropriate
    chunker based on the document's file extension to splitting the document into manageable chunks.

    - Extension-based Chunking: The class uses `ChunkerFactory` to determine the correct chunker based on the file extension.
    - Error Handling: The class includes mechanisms to handle and log errors, including general errors and specific timeout-related errors.

    Timeout Management:
    -------------------
    - max_time: The maximum allowed time for the chunking process is set to 230 seconds to avoid web application timeouts.
    - Timeout Check: The class checks if the chunking process exceeds the maximum allowed time and handles the timeout scenario by generating an appropriate error message.

    Error Messages:
    ---------------
    - Generates specific error messages for different scenarios, including timeouts and general processing errors.
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
        self.max_time = 230  # webapp timeout is 230 seconds

    def _check_timeout(self, start_time):
        """Check if the operation has timed out."""
        elapsed_time = time.time() - start_time
        return elapsed_time > self.max_time

    def _error_message(self, error_type="general", exception=None, filename=""):
        """Generate an error message based on the error type."""
        if error_type == 'timeout':
            error_message = (
                "Terminating the function so it doesn't run indefinitely. "
                "The AI Search indexer's timeout is 3m50s. If the document is large "
                "(more than 100 pages), try dividing it into smaller files. If you are "
                "encountering many 429 errors in the function log, try increasing the "
                "embedding model's quota as the retrial logic delays processing."
            )
        else:
            error_message = "An error occurred while processing the document."
            if exception is not None:
                error_message += f"Exception: {str(exception)}"

        logging.info(f"[document_chunking]{f'[{filename}]' if filename else ''} Error: {error_message}, Ingested Document: {f'[{filename}]' if filename else ''}")

        return error_message

    def chunk_document(self, data):
        """Chunk the document into smaller parts."""
        chunks = []
        errors = []
        warnings = []
        start_time = time.time()

        url = data['documentUrl']
        filename = get_filename(url)
        extension = get_file_extension(url)
        
        try:
            chunker = ChunkerFactory().get_chunker(extension, data)
            chunks = chunker.get_chunks()
        except Exception as e:
            errors.append(self._error_message(exception=e, filename=filename))

        elapsed_time = time.time() - start_time
        logging.info(
            f"[document_chunking][{filename}] Finished chunking in {elapsed_time:.2f} seconds. "
            f"{len(chunks)} chunks. {len(errors)} errors. {len(warnings)} warnings."
        )
        return chunks, errors, warnings