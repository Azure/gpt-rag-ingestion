import logging
import os
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

    Attributes:
    -----------
    - blob_client (BlobStorageClient): Used to download the spreadsheet data from blob storage.
    - max_chunk_size (int): Maximum allowed tokens per chunk to ensure chunks do not exceed a specified size.
    - document_content (str): The content of the spreadsheet after processing.

    Methods:
    --------
    - get_chunks():
        Splits the spreadsheet content into chunks. Each sheet is converted into an appropriate format 
        (HTML, Markdown, or summary) based on its size. The method logs the process and creates chunks 
        that include the sheet's table content, summary, and title.

    - _spreadsheet_process():
        Processes each sheet in the spreadsheet. Converts the content to HTML or Markdown depending on 
        token size. If the content exceeds token limits, a summary is generated. The method returns a 
        list of dictionaries, where each dictionary represents a sheet and is used as the basis for a 
        chunk. Each dictionary contains the sheet name, table content, and summary.

    - _excel_to_markdown():
        Converts a sheet from the spreadsheet into Markdown format. It reads the data from each row and 
        cell, handling empty values and formatting the content into a Markdown table using the `tabulate` library.

    - _excel_to_html():
        Converts a sheet from the spreadsheet into HTML format. Handles merged cells by mapping them to the 
        appropriate `rowspan` and `colspan` attributes. Processes each row and cell to generate a well-formatted 
        HTML table.

    The SpreadsheetChunker class is designed to break down large spreadsheet documents into smaller, more manageable 
    pieces for efficient processing, analysis, and summarization. It ensures that even complex spreadsheet structures, 
    such as merged cells, are correctly handled during chunking, and that each resulting dictionary is ready to be 
    used as the basis for a chunk.
    """

    def __init__(self, data, max_chunk_size=None):
        """
        Initializes the SpreadsheetChunker with the given data and sets up chunking parameters from environment variables.
        
        Args:
            data (str): The spreadsheet content to be chunked.
        """
        super().__init__(data)
        max_chunk_size = int(os.getenv("SPREADSHEET_NUM_TOKENS", 20000)) if max_chunk_size is None else int(max_chunk_size)
        self.max_chunk_size = max_chunk_size

    def get_chunks(self):
        chunks = [] 
        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks] Running get_chunks.")
        total_start_time = time.time()

        # Extract the relevant text from the spreadsheet
        sheets = self._spreadsheet_process()
        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks] workbook has {len(sheets)} sheets")

        chunk_id = 0
        for sheet in sheets:      
            start_time = time.time()
            chunk_id += 1
            logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Starting processing chunk {chunk_id} sheet.")            
            chunk_dict = self._create_chunk(
                chunk_id=chunk_id,
                content=sheet["table"],
                summary=sheet["summary"],
                embedding_text=sheet["summary"] if sheet["summary"] else sheet["table"],
                title=sheet["name"]
            )            
            chunks.append(chunk_dict)
            elapsed_time = time.time() - start_time
            logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks][{sheet['name']}] Processed chunk {chunk_id} in {elapsed_time:.2f} seconds.")            

        total_elapsed_time = time.time() - total_start_time
        logging.info(f"[spreadsheet_chunker][{self.filename}][get_chunks] Finished get_chunks. Created {len(chunks)} chunks in {total_elapsed_time:.2f} seconds.")

        return chunks

    def _spreadsheet_process(self):
        """
        Downloads a spreadsheet from Azure Blob Storage, processes each sheet into HTML or markdown tables, and generates 
        summaries if token limits are exceeded. Each sheet's processed data is stored in a dictionary, which will be used as 
        the basis for a chunk.

        Steps:
        1. Download and load the spreadsheet with `openpyxl`.
        2. For each sheet:
            - Convert the content to an HTML table and estimate token count.
            - If the HTML exceeds token limits, convert to markdown or generate a summary.
            - Store the resulting HTML, markdown, or summary in the `table` field and any generated summary in `summary`.

        Returns:
            list[dict]: A list of dictionaries for each sheet:
            - `name`: Sheet name.
            - `table`: The HTML, markdown, or summary.
            - `summary`: A summary if needed, otherwise empty.

        Fields/Attributes:
        - `blob_client`: For downloading the spreadsheet.
        - `token_estimator`: For estimating token counts.
        - `aoai_client`: For generating summaries with OpenAI.
        - `max_chunk_size`: Maximum tokens per chunk.
        - `max_embeddings_model_input_tokens`: Maximum tokens for embedding models.
        """        
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] starting blob download.")        
        blob_data = self.blob_client.download_blob()
        blob_stream = BytesIO(blob_data)
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] starting openpyxl load_workbook.")                     
        workbook = load_workbook(blob_stream, data_only=True)

        # Process each sheet in the workbook
        sheets = []
        total_start_time = time.time()
        
        for sheet_name in workbook.sheetnames:
            logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}] started processing.")                   
            start_time = time.time()
            sheet_dict = {}            
            sheet_dict['name'] = sheet_name
            sheet = workbook[sheet_name]
            
            # initialize field logic variables
            html_table = self._excel_to_html(sheet)
            html_table_tokens = self.token_estimator.estimate_tokens(html_table)
            markdown_table = ""
            markdown_table_tokens = 0
            summary = ""

            # summary field logic
            if html_table_tokens > self.aoai_client.max_embeddings_model_input_tokens:
                logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}]. HTML table has {html_table_tokens} tokens. Max embeddings tokens is {self.aoai_client.max_embeddings_model_input_tokens}. Generating markdown.")
                markdown_table = self._excel_to_markdown(sheet)     
                markdown_table_tokens = self.token_estimator.estimate_tokens(markdown_table)                
                if markdown_table_tokens > self.aoai_client.max_embeddings_model_input_tokens:
                    logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}]. Markdown table has {markdown_table_tokens} tokens. Generating summary.")
                    prompt = f"Summarize the markdown table provided.\ntable_content: \n{markdown_table} "
                    summary = self.aoai_client.get_completion(prompt, max_tokens=4096)
                else:
                    logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}]. Markdown table has {markdown_table_tokens} tokens. No summary needed.")                
                    summary = ""
            else:
                logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}]. HTML table has {html_table_tokens} tokens. No summary needed.")                
                summary = ""
            sheet_dict["summary"] = summary

            # table field logic
            if html_table_tokens < self.max_chunk_size:
                logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}].  HTML table has {html_table_tokens} tokens. Max tokens is {self.max_chunk_size}.")
                sheet_dict["table"] = html_table
            else:
                logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}].  HTML table has {html_table_tokens} tokens. Max tokens is {self.max_chunk_size} Converting to markdown.")
                markdown_table = self._excel_to_markdown(sheet) if markdown_table == "" else markdown_table
                markdown_table_tokens = self.token_estimator.estimate_tokens(markdown_table) if markdown_table_tokens == 0 else markdown_table_tokens
                if markdown_table_tokens < self.max_chunk_size:
                    sheet_dict["table"] = markdown_table
                else:
                    logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_name}].  Markdown table has {markdown_table_tokens} tokens. Max tokens is {self.max_chunk_size} Using summary as the content.")
                    prompt = f"Summarize the markdown table provided.\ntable_content: \n{markdown_table} "
                    summary = self.aoai_client.get_completion(prompt, max_tokens=4096) if summary == "" else summary
                    sheet_dict["table"] = summary

            elapsed_time = time.time() - start_time
            logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process][{sheet_dict['name']}] processed in {elapsed_time:.2f} seconds.")
            sheets.append(sheet_dict)
        
        total_elapsed_time = time.time() - total_start_time
        logging.info(f"[spreadsheet_chunker][{self.filename}][spreadsheet_process] Total processing time: {total_elapsed_time:.2f} seconds.")
        
        return sheets

    def _excel_to_markdown(self, sheet):
        # Read the data and determine cell colors
        data = []
        for row in sheet.iter_rows():
            row_data = []
            for cell in row:
                cell_value = cell.value
                if cell_value is None:
                    cell_value = ""
                cell_text = str(cell_value)
                row_data.append(cell_text)
            if "".join(row_data)!="":
                data.append(row_data)

        # Get the header from the first row
        headers = [cell.value if cell.value is not None else "" for cell in sheet[1]]
        table = tabulate(data, headers, tablefmt="pipe")
        return table
    
    def _excel_to_html(self, sheet):
        html = '<table border="1">'
        
        # Dictionary to track merged cells
        merged_cells = {}
        
        # Process merged cells to map them to colspan and rowspan
        for merged_cell in sheet.merged_cells.ranges:
            min_col, min_row, max_col, max_row = merged_cell.min_col, merged_cell.min_row, merged_cell.max_col, merged_cell.max_row
            merged_cells[(min_row, min_col)] = (max_row - min_row + 1, max_col - min_col + 1)
        
        # Iterate over rows and columns to build the HTML
        for row in sheet.iter_rows():
            html += '  <tr>'
            for cell in row:
                row_num = cell.row
                col_num = cell.column
                
                # Check if the cell is the top-left of a merged cell
                if (row_num, col_num) in merged_cells:
                    rowspan, colspan = merged_cells[(row_num, col_num)]
                    cell_value = '' if cell.value is None else cell.value
                    html += f'    <td rowspan="{rowspan}" colspan="{colspan}">{cell_value}</td>'
                else:
                    # Skip cells that are part of a merged range but not the top-left
                    is_merged = False
                    for key, (rspan, cspan) in merged_cells.items():
                        start_row, start_col = key
                        if start_row <= row_num < start_row + rspan and start_col <= col_num < start_col + cspan:
                            is_merged = True
                            break
                    
                    if not is_merged:
                        cell_value = '' if cell.value is None else cell.value
                        html += f'    <td>{cell_value}</td>'
                    
            html += '  </tr>'
        
        html += '</table>'
        html = html.replace('\n', '').replace('\t', '')
        return html