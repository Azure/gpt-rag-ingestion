import io
import logging
import os
import re
import tempfile
from typing import Generator, Optional

from pypdf import PdfReader, PdfWriter


def get_file_extension(file_path: str) -> Optional[str]:
    file_path = os.path.basename(file_path)
    return file_path.split(".")[-1].lower()

def get_filename(file_path: str) -> str:
    return file_path.rsplit('/', 1)[-1]

def get_filename_from_data(data: dict) -> str:
    """
    Extracts the filename from a given data dictionary.

    The function checks if the dictionary contains the key 'fileName'.
    If it does, it returns the value associated with 'fileName'.
    Otherwise, it extracts the filename from the 'documentUrl' key by splitting the URL and taking the last part.

    Args:
        data (dict): A dictionary containing file information. It should have either a 'fileName' key or a 'documentUrl' key.

    Returns:
        str: The extracted filename.

    Raises:
        KeyError: If neither 'fileName' nor 'documentUrl' keys are present in the dictionary.
    """
    if data.get('fileName'):
        filename = data['fileName']
    else:
        filename = data['documentUrl'].split('/')[-1]
    return filename

def get_filepath_from_data(data: dict) -> str:
    """
    Extracts the file path from the document URL in the provided data dictionary.
    
    The function assumes that the URL is structured as:
    https://<account>.blob.core.windows.net/<container>/<optional folders>/filename
    
    It removes the container part and returns the rest as the relative file path.
    
    Examples:
    - URL: "https://.../container01/surface-pro-4-user-guide-EN.pdf"
      Result: "surface-pro-4-user-guide-EN.pdf"
    - URL: "https://.../container02/somefolder/surface-pro-4-user-guide-PT.pdf"
      Result: "somefolder/surface-pro-4-user-guide-PT.pdf"
    """
    url = data['documentUrl']
    # Split the URL by '/'
    parts = url.split('/')
    
    # The container is at index 3 after splitting (indices: 0:"https:", 1:"", 2: host, 3: container)
    # The remaining parts form the relative file path.
    filepath = '/'.join(parts[4:])
    
    return filepath


# ---------------------------------------------------------------------------
# PDF page counting and splitting
# ---------------------------------------------------------------------------

def get_pdf_page_count(file_bytes: bytes) -> int:
    """Return the number of pages in a PDF from its raw bytes."""
    reader = PdfReader(io.BytesIO(file_bytes))
    return len(reader.pages)


def split_pdf_to_temp_files(
    source_path: str,
    max_pages: int = 300,
) -> Generator[str, None, None]:
    """Split a PDF on disk into temp-file parts of at most *max_pages* pages.

    Yields the path of each temporary PDF file.  The caller is responsible
    for deleting each temp file after use.

    If the PDF has <= *max_pages* pages, yields *source_path* itself
    (no splitting, no temp file created).
    """
    reader = PdfReader(source_path)
    total = len(reader.pages)

    if total <= max_pages:
        yield source_path
        return

    parts = (total + max_pages - 1) // max_pages
    logging.info(
        f"[pdf_split] Splitting {os.path.basename(source_path)}: "
        f"{total} pages into {parts} parts of up to {max_pages} pages"
    )

    for part_idx in range(parts):
        start = part_idx * max_pages
        end = min(start + max_pages, total)
        writer = PdfWriter()
        for page_num in range(start, end):
            writer.add_page(reader.pages[page_num])

        tmp = tempfile.NamedTemporaryFile(
            suffix=".pdf", prefix=f"pdfsplit_p{part_idx}_", delete=False
        )
        try:
            writer.write(tmp)
            tmp.close()
            yield tmp.name
        except Exception:
            tmp.close()
            _safe_delete(tmp.name)
            raise


def renumber_page_markers(markdown: str, page_offset: int) -> str:
    """Shift ``<!-- PageBreak -->`` markers in *markdown* by *page_offset*.

    Content Understanding and Document Intelligence emit
    ``<!-- PageBreak -->`` (unnumbered) between pages.  When multiple
    parts of a split PDF are concatenated, the second part's markers must
    be offset so that ``_number_pagebreaks`` (called later) produces
    correct absolute page numbers.

    This function prepends *page_offset* synthetic ``<!-- PageBreak -->``
    markers at the beginning of *markdown* so that the downstream
    numbering pass counts correctly.
    """
    if page_offset <= 0:
        return markdown
    prefix = "<!-- PageBreak -->\n" * page_offset
    return prefix + markdown


def save_bytes_to_temp_file(data: bytes, suffix: str = ".pdf") -> str:
    """Write *data* to a temp file and return its path.

    The caller is responsible for deleting the file when done.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="ingest_", delete=False)
    try:
        tmp.write(data)
        tmp.close()
        return tmp.name
    except Exception:
        tmp.close()
        _safe_delete(tmp.name)
        raise


def _safe_delete(path: str) -> None:
    """Delete a file if it exists, ignoring errors."""
    try:
        if path and os.path.exists(path):
            os.unlink(path)
    except OSError:
        pass
