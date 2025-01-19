class UnsupportedFormatError(Exception):
    """Exception raised when a format is not supported."""
    pass

class DocIntNotAvailableError(Exception):
    "Exception raised when Document Intelligence 4.0 API is not available."
    pass

class DocAnalysisError(Exception):
    "Exception raised when document analysis fails."
    pass
