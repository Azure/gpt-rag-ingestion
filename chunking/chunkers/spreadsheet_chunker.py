import logging 
import os
import time

from io import BytesIO

from openpyxl import load_workbook
from tabulate import tabulate

from .base_chunker import BaseChunker

class SpreadsheetChunker(BaseChunker):
    """
    SpreadsheetChunker processes and chunks spreadsheet content, such as Excel files, into manageable pieces for analysis and summarization. 
    It handles both chunking by rows or sheets, allowing users to specify whether to include header rows in each chunk, and ensures that 
    the content size does not exceed a specified token limit.

    The class supports the following operations:
    - Converts spreadsheets into chunkable content.
    - Provides options to chunk either by row or by sheet.
    - Includes optional header rows in chunks.
    - Summarizes large sheets if the content exceeds the maximum chunk size.
    
    Attributes:
    -----------
    max_chunk_size (int): Maximum allowed size of each chunk in tokens.
    chunking_by_row (bool): Whether to chunk by row instead of by sheet.
    include_header_in_chunks (bool): Whether to include header rows in each row-based chunk.
    document_content (str): Processed spreadsheet content ready for chunking.
    blob_client (BlobStorageClient): Client for downloading spreadsheet data from blob storage.

    Methods:
    --------
    - get_chunks(): Splits the spreadsheet content into manageable chunks, based on the configuration.
    - _spreadsheet_process(): Extracts and processes data from each sheet, including summaries if necessary.
    - _get_sheet_data(sheet): Retrieves data and headers from the given sheet, handling empty cells.
    - _clean_markdown_table(table_str): Cleans up Markdown table strings by removing excessive whitespace.
    """

    def __init__(self, data, max_chunk_size=None, chunking_by_row=None, include_header_in_chunks=None):
        """
        Initializes the SpreadsheetChunker with the provided data and environment configurations.
        
        Args:
            data (str): The spreadsheet content to be chunked.
            max_chunk_size (int, optional): Maximum allowed size of each chunk in tokens. Defaults to an environment variable 'SPREADSHEET_NUM_TOKENS' or 0 if not set.
            chunking_by_row (bool, optional): Whether to chunk by row instead of by sheet. Defaults to an environment variable 'CHUNKING_BY_ROW' or False.
            include_header_in_chunks (bool, optional): Whether to include the header row in each chunk if chunking by row. Defaults to 'INCLUDE_HEADER_IN_CHUNKS' environment variable or False.
        """
        super().__init__(data)
        
        if max_chunk_size is None:
            self.max_chunk_size = int(os.getenv("SPREADSHEET_NUM_TOKENS", 0))
        else:
            self.max_chunk_size = int(max_chunk_size)
        
        if chunking_by_row is None:
            chunking_env = os.getenv("SPREADSHEET_CHUNKING_BY_ROW", "false").lower()
            self.chunking_by_row = chunking_env in ["true", "1", "yes"]
        else:
            self.chunking_by_row = bool(chunking_by_row)
        
        if include_header_in_chunks is None:
            include_header_env = os.getenv("SPREADSHEET_CHUNKING_BY_ROW_INCLUDE_HEADER", "false").lower()
            self.include_header_in_chunks = include_header_env in ["true", "1", "yes"]
        else:
            self.include_header_in_chunks = bool(include_header_in_chunks)

    def get_chunks(self):
        """
        Splits the spreadsheet content into smaller chunks. Depending on the configuration, chunks can be created by sheet or by row.
        - If chunking by sheet, the method summarizes content that exceeds the maximum chunk size.
        - If chunking by row, each row is processed into its own chunk, optionally including the header row.
        
        Returns:
            List[dict]: A list of dictionaries representing the chunks created from the spreadsheet.
        """
        chunks = [] 
        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks] Running get_chunks.")
        total_start_time = time.time()

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

                table_content = self._clean_markdown_table(table_content)
                table_tokens = self.token_estimator.estimate_tokens(table_content)
                
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
                    if not any(cell.strip() for cell in row):
                        continue
                    chunk_id += 1
                    start_time = time.time()
                    logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Processing chunk {chunk_id} for row {row_index}.")
                    
                    if self.include_header_in_chunks:
                        table = tabulate([headers, row], headers="firstrow", tablefmt="github")
                    else:
                        table = tabulate([row], headers=headers, tablefmt="github")
                    
                    table = self._clean_markdown_table(table)
                    summary = ""
                    
                    table_tokens = self.token_estimator.estimate_tokens(table)
                    if self.max_chunk_size > 0 and table_tokens > self.max_chunk_size:
                        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Row table has {table_tokens} tokens. Max tokens is {self.max_chunk_size}. Truncating content.")
                        content = table
                        embedding_text = table
                    else:
                        content = table
                        embedding_text = table

                    chunk_dict = self._create_chunk(
                        chunk_id=chunk_id,
                        content=content,
                        summary=summary,
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
        Extracts and processes each sheet from the spreadsheet, converting the content into Markdown table format. 
        If chunking by sheet, a summary is generated if the sheet's content exceeds the maximum token size.

        Returns:
            List[dict]: A list of dictionaries, where each dictionary contains sheet metadata, headers, rows, table content, and a summary if applicable.
        """
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] Starting blob download.")        
        blob_data = self.blob_client.download_blob()
        blob_stream = BytesIO(blob_data)
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] Starting openpyxl load_workbook.")                    
        workbook = load_workbook(blob_stream, data_only=True)

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
            table = self._clean_markdown_table(table)
            sheet_dict["table"] = table
            
            if not self.chunking_by_row:
                prompt = f"Summarize the table with data in it, by understanding the information clearly.\n table_data:{table}"
                summary = self.aoai_client.get_completion(prompt, max_tokens=2048)
                sheet_dict["summary"] = summary
                logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_dict['name']}] Generated summary.")
            else:
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
        Retrieves data and headers from the given sheet. Each row's data is processed into a list format, ensuring that empty rows are excluded.

        Args:
            sheet (Worksheet): The worksheet object to extract data from.

        Returns:
            Tuple[List[List[str]], List[str]]: A tuple containing a list of row data and a list of headers.
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
            if set(line.strip()) <= set('-| '):
                cleaned_lines.append(line)
                continue

            cells = line.split('|')
            stripped_cells = [cell.strip() for cell in cells[1:-1]]
            cleaned_line = '| ' + ' | '.join(stripped_cells) + ' |'
            cleaned_lines.append(cleaned_line)

        return '\n'.join(cleaned_lines)