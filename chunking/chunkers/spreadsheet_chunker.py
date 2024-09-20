import logging 
import os
import re
import time

from io import BytesIO

from openpyxl import load_workbook
from tabulate import tabulate

from .base_chunker import BaseChunker

class SpreadsheetChunker(BaseChunker):
    """
    SpreadsheetChunker processes and chunks spreadsheet content, such as Excel files, into manageable pieces. It handles 
    various spreadsheet structures and converts them into formats suitable for chunking and summarization.

    Initialization:
    ---------------
    The SpreadsheetChunker is initialized with the following parameters:
    - data (str): The spreadsheet content to be chunked.
    - max_chunk_size (int, optional): The maximum size of each chunk in tokens.
    - chunking_by_row (bool, optional): If set to True, chunks are created per row instead of per sheet. Defaults to False.
      This can also be set via the environment variable 'CHUNKING_BY_ROW'.
    - include_header_in_chunks (bool, optional): If set to True and chunking by row, each chunk will include the header row and the respective row.
      Defaults to False. This can also be set via the environment variable 'INCLUDE_HEADER_IN_CHUNKS'.

    Attributes:
    -----------
    - blob_client (BlobStorageClient): Used to download the spreadsheet data from blob storage.
    - max_chunk_size (int): Maximum allowed tokens per chunk to ensure chunks do not exceed a specified size.
    - chunking_by_row (bool): Determines whether to chunk by sheet or by row.
    - include_header_in_chunks (bool): Determines whether to include headers in each row-based chunk.
    - document_content (str): The content of the spreadsheet after processing.

    Methods:
    --------
    - get_chunks():
        Splits the spreadsheet content into chunks. Each sheet's data is processed and summarized 
        if necessary, depending on its size. The method logs the process and creates chunks that 
        include either the entire sheet or individual rows based on the chunking_by_row flag.

    - _spreadsheet_process():
        Processes each sheet in the spreadsheet. The sheet's data is converted into a grid-like 
        table format, and if chunking by sheet and the table exceeds a token limit, a summary is generated using an AI model. 
        This method returns a list of dictionaries where each dictionary represents a sheet and is 
        used as the basis for creating chunks. Each dictionary contains the sheet's name, table content, 
        and summary (if not chunking by row).

    - _get_sheet_data(sheet):
        Retrieves data from the given sheet and processes it into a list format for each row and cell. Handles 
        empty values, ensuring that only non-empty rows are included. Also returns headers from the first row.
    
    The SpreadsheetChunker class is designed to break down large spreadsheet documents into smaller, more manageable 
    pieces for efficient processing, analysis, and summarization. It ensures that complex spreadsheet structures 
    are correctly handled during chunking, and that each resulting dictionary is ready to be used as the basis 
    for a chunk.
    """

    def __init__(self, data, max_chunk_size=None, chunking_by_row=None, include_header_in_chunks=None):
        """
        Initializes the SpreadsheetChunker with the given data and sets up chunking parameters from environment variables.
        
        Args:
            data (str): The spreadsheet content to be chunked.
            max_chunk_size (int, optional): The maximum allowed size of each chunk in tokens. Defaults to the 
                                            environment variable 'SPREADSHEET_NUM_TOKENS' or 0 if not set.
            chunking_by_row (bool, optional): If set to True, chunks are created per row instead of per sheet.
                                              Defaults to the environment variable 'CHUNKING_BY_ROW' or False if not set.
            include_header_in_chunks (bool, optional): If set to True and chunking by row, each chunk will include the header row and the respective row.
                                                     Defaults to the environment variable 'INCLUDE_HEADER_IN_CHUNKS' or False if not set.
        """
        super().__init__(data)
        
        # Set max_chunk_size from parameter or environment variable
        if max_chunk_size is None:
            self.max_chunk_size = int(os.getenv("SPREADSHEET_NUM_TOKENS", 0))
        else:
            self.max_chunk_size = int(max_chunk_size)
        
        # Set chunking_by_row from parameter or environment variable
        if chunking_by_row is None:
            chunking_env = os.getenv("SPREADSHEET_CHUNKING_BY_ROW", "false").lower()
            self.chunking_by_row = chunking_env in ["true", "1", "yes"]
        else:
            self.chunking_by_row = bool(chunking_by_row)
        
        # Set include_header_in_chunks from parameter or environment variable
        if include_header_in_chunks is None:
            include_header_env = os.getenv("SPREADSHEET_CHUNKING_BY_ROW_INCLUDE_HEADER", "false").lower()
            self.include_header_in_chunks = include_header_env in ["true", "1", "yes"]
        else:
            self.include_header_in_chunks = bool(include_header_in_chunks)

    def get_chunks(self):
        """
        [Existing docstring]
        """
        chunks = [] 
        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks] Running get_chunks.")
        total_start_time = time.time()

        # Extract the relevant text from the spreadsheet
        sheets = self._spreadsheet_process()
        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks] Workbook has {len(sheets)} sheets")

        chunk_id = 0
        for sheet in sheets:
            if not self.chunking_by_row:
                # Original behavior: Chunk per sheet
                start_time = time.time()
                chunk_id += 1
                logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Starting processing chunk {chunk_id} (sheet).")
                table_content = sheet["table"]

                # Clean the table to reduce whitespace
                table_content = self._clean_markdown_table(table_content)

                table_tokens = self.token_estimator.estimate_tokens(table_content)
                # If max_chunk_size is defined and the table content exceeds the maximum chunk size, use the summary instead
                if self.max_chunk_size > 0 and table_tokens > self.max_chunk_size:
                    logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Table has {table_tokens} tokens. Max tokens is {self.max_chunk_size}. Using summary.")
                    table_content = sheet["summary"]
                chunk_dict = self._create_chunk(
                    chunk_id=chunk_id,
                    content=table_content,
                    summary=sheet["summary"] if not self.chunking_by_row else "",
                    embedding_text=sheet["summary"] if (sheet["summary"] and not self.chunking_by_row) else table_content,
                    title=sheet["name"]
                )            
                chunks.append(chunk_dict)
                elapsed_time = time.time() - start_time
                logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Processed chunk {chunk_id} in {elapsed_time:.2f} seconds.")            
            else:
                # New behavior: Chunk per row
                logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Starting row-wise chunking.")
                headers = sheet.get("headers", [])
                rows = sheet.get("data", [])
                for row_index, row in enumerate(rows, start=1):
                    # Skip empty rows
                    if not any(cell.strip() for cell in row):
                        continue
                    chunk_id += 1
                    start_time = time.time()
                    logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Processing chunk {chunk_id} for row {row_index}.")
                    
                    if self.include_header_in_chunks:
                        # Include headers in the chunk
                        table = tabulate([headers, row], headers="firstrow", tablefmt="github")
                    else:
                        # Include only the row in the chunk
                        table = tabulate([row], headers=headers, tablefmt="github")
                    
                    # Clean the table to reduce whitespace
                    table = self._clean_markdown_table(table)
                    
                    # No summary generation for row chunks (performance reasons)
                    summary = ""
                    
                    # Estimate tokens for the table
                    table_tokens = self.token_estimator.estimate_tokens(table)
                    if self.max_chunk_size > 0 and table_tokens > self.max_chunk_size:
                        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Row table has {table_tokens} tokens. Max tokens is {self.max_chunk_size}. Truncating content.")
                        # Optionally, handle truncation here if necessary
                        # For simplicity, we'll keep the table as is
                        content = table
                        embedding_text = table
                    else:
                        content = table
                        embedding_text = table

                    chunk_dict = self._create_chunk(
                        chunk_id=chunk_id,
                        content=content,
                        summary=summary,  # Empty summary
                        embedding_text=embedding_text,
                        title=f"{sheet['name']} - Row {row_index}"
                    )
                    chunks.append(chunk_dict)
                    elapsed_time = time.time() - start_time
                    logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Processed chunk {chunk_id} in {elapsed_time:.2f} seconds.")
        
        total_elapsed_time = time.time() - total_start_time
        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks] Finished get_chunks. Created {len(chunks)} chunks in {total_elapsed_time:.2f} seconds.")

        return chunks

    def _spreadsheet_process(self):
        """
        [Existing docstring]
        """
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] Starting blob download.")        
        blob_data = self.blob_client.download_blob()
        blob_stream = BytesIO(blob_data)
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] Starting openpyxl load_workbook.")                    
        workbook = load_workbook(blob_stream, data_only=True)

        # Process each sheet in the workbook
        sheets = []
        total_start_time = time.time()
    
        for sheet_name in workbook.sheetnames:
            logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}] Started processing.")                  
            start_time = time.time()
            sheet_dict = {}            
            sheet_dict['name'] = sheet_name
            sheet = workbook[sheet_name]
            data, headers = self._get_sheet_data(sheet)
            sheet_dict["headers"] = headers
            sheet_dict["data"] = data
            table = tabulate(data, headers=headers, tablefmt="grid")
            
            # Clean the table to reduce whitespace
            table = self._clean_markdown_table(table)
            
            sheet_dict["table"] = table
            
            if not self.chunking_by_row:
                # Generate summary only if not chunking by row
                prompt = f"Summarize the table with data in it, by understanding the information clearly.\n table_data:{table}"
                summary = self.aoai_client.get_completion(prompt, max_tokens=2048)
                sheet_dict["summary"] = summary
                logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_dict['name']}] Generated summary.")
            else:
                # Do not generate summary if chunking by row
                sheet_dict["summary"] = ""
                logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_dict['name']}] Skipped summary generation (chunking by row).")
            
            elapsed_time = time.time() - start_time
            logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_dict['name']}] Processed in {elapsed_time:.2f} seconds.")
            sheets.append(sheet_dict)
    
        total_elapsed_time = time.time() - total_start_time
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] Total processing time: {total_elapsed_time:.2f} seconds.")

        return sheets

    def _get_sheet_data(self, sheet):
        """
        [Existing docstring]
        """
        data = []
        for row in sheet.iter_rows(min_row=2):  # Start from the second row to skip headers
            row_data = []
            for cell in row:
                cell_value = cell.value
                if cell_value is None:
                    cell_value = ""
                cell_text = str(cell_value)
                row_data.append(cell_text)
            if "".join(row_data).strip() != "":
                data.append(row_data)

        # Get the header from the first row
        headers = [cell.value if cell.value is not None else "" for cell in sheet[1]]

        return data, headers
    
    def _clean_markdown_table(self, table_str):
        """
        Cleans up a Markdown table string by removing excessive whitespace from each cell.

        Args:
            table_str (str): The Markdown table string to be cleaned.

        Returns:
            str: The cleaned Markdown table string with reduced whitespace.
        """
        cleaned_lines = []
        lines = table_str.splitlines()

        for line in lines:
            # Identify the separator line (e.g., |---|---|) and keep it unchanged
            if set(line.strip()) <= set('-| '):
                cleaned_lines.append(line)
                continue

            # Split the line into cells based on the '|' delimiter
            cells = line.split('|')

            # Strip leading and trailing whitespace from each cell
            # Exclude the first and last elements if they are empty (due to leading/trailing '|')
            stripped_cells = [cell.strip() for cell in cells[1:-1]]

            # Reconstruct the line with single space padding around each cell
            cleaned_line = '| ' + ' | '.join(stripped_cells) + ' |'
            cleaned_lines.append(cleaned_line)

        # Join all cleaned lines back into a single string
        return '\n'.join(cleaned_lines)
