import os
import re
from typing import Optional

def get_file_extension(file_path: str) -> Optional[str]:
    file_path = os.path.basename(file_path)
    return file_path.split(".")[-1]

def get_filename(file_path: str) -> str:
    match = re.search(r'documents/(.*/)?(.*)', file_path)
    filepath = ""
    if match:
        filepath = (match.group(1) or '') + (match.group(2) or '')
    return filepath