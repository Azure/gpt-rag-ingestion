import os
import re
from typing import Optional

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