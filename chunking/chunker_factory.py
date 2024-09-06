import logging

from .chunkers.doc_analysis_chunker import DocAnalysisChunker
from .chunkers.langchain_chunker import LangChainChunker
from .chunkers.spreadsheet_chunker import SpreadsheetChunker
from .chunkers.transcription_chunker import TranscriptionChunker

from tools import DocumentIntelligenceClient

class ChunkerFactory:
    """Factory class to create appropriate chunker based on file extension."""
    
    def __init__(self):
        docint_client = DocumentIntelligenceClient()
        self.docint_40_api = docint_client.docint_40_api 

    def get_chunker(self, extension, data):
        """
        Get the appropriate chunker based on the file extension.

        Args:
            extension (str): The file extension.
            data (dict): The data containing document information.

        Returns:
            BaseChunker: An instance of a chunker class.
        """
        filename = data['documentUrl'].split('/')[-1]
        logging.info(f"[chunker_factory][{filename}] Creating chunker")

        if extension == 'vtt':
            return TranscriptionChunker(data)
        elif extension in ('xlsx', 'xls'):
            return SpreadsheetChunker(data)
        elif extension in ('pdf', 'png', 'jpeg', 'jpg', 'bmp', 'tiff'):
            return DocAnalysisChunker(data)
        elif extension in ('docx', 'pptx'):
            if self.docint_40_api:
                return DocAnalysisChunker(data)
            else:
                logging.info(f"[chunker_factory][{filename}] Processing 'pptx' and 'docx' files requires Doc Intelligence 4.0.")                
                raise RuntimeError("Processing 'pptx' and 'docx' files requires Doc Intelligence 4.0.")
        else:
            return LangChainChunker(data)
        