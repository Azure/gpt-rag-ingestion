import logging
import os

from .chunkers.doc_analysis_chunker import DocAnalysisChunker
from .chunkers.multimodal_chunker import MultimodalChunker
from .chunkers.langchain_chunker import LangChainChunker
from .chunkers.spreadsheet_chunker import SpreadsheetChunker
from .chunkers.transcription_chunker import TranscriptionChunker
from .chunkers.nl2sql_chunker import NL2SQLChunker

from tools import DocumentIntelligenceClient
from utils import get_filename_from_data, get_file_extension

class ChunkerFactory:
    """Factory class to create appropriate chunker based on file extension."""
    
    def __init__(self):
        docint_client = DocumentIntelligenceClient()
        self.docint_40_api = docint_client.docint_40_api 
        _multimodality = os.getenv("MULTIMODAL", "false").lower()
        self.multimodality = _multimodality in ["true", "1", "yes"]

    def get_chunker(self, data):
        """
        Get the appropriate chunker based on the file extension.

        Args:
            extension (str): The file extension.
            data (dict): The data containing document information.

        Returns:
            BaseChunker: An instance of a chunker class.
        """
        filename = get_filename_from_data(data)
        logging.info(f"[chunker_factory][{filename}] Creating chunker")

        extension = get_file_extension(filename)
        if extension == 'vtt':
            return TranscriptionChunker(data)
        elif extension in ('xlsx', 'xls'):
            return SpreadsheetChunker(data)
        elif extension in ('pdf', 'png', 'jpeg', 'jpg', 'bmp', 'tiff'):
            if self.multimodality:
                return MultimodalChunker(data)
            else:
                return DocAnalysisChunker(data)
        elif extension in ('docx', 'pptx'):
            if self.docint_40_api:
                if self.multimodality:
                    return MultimodalChunker(data)
                else:
                    return DocAnalysisChunker(data)
            else:
                logging.info(f"[chunker_factory][{filename}] Processing 'pptx' and 'docx' files requires Doc Intelligence 4.0.")
                raise RuntimeError("Processing 'pptx' and 'docx' files requires Doc Intelligence 4.0.")
        elif extension in ('nl2sql'):
            return NL2SQLChunker(data)
        else:
            return LangChainChunker(data)
        
    @staticmethod
    def get_supported_extensions():
        """
        Get a comma-separated list of supported file extensions.

        Returns:
            str: A comma-separated list of supported file extensions.
        """
        extensions = [
            'vtt',
            'xlsx', 'xls',
            'pdf', 'png', 'jpeg', 'jpg', 'bmp', 'tiff',
            'docx', 'pptx', 'json'
        ]
        return ', '.join(extensions)
