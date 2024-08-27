import logging
from tools import BlobStorageClient
from .base_chunker import BaseChunker
from openpyxl import load_workbook
from tabulate import tabulate
from io import BytesIO

class SpreadsheetChunker(BaseChunker):
    def __init__(self, data, max_chunk_size=None):
        """
        Initializes the SpreadsheetChunker with the given data and sets up chunking parameters from environment variables.
        
        Args:
            data (str): The spreadsheet content to be chunked.
        """
        super().__init__(data)
        self.blob_client = BlobStorageClient()
        self.max_chunk_size = max_chunk_size or 4096

    def get_chunks(self):           
        chunks = [] 
        logging.info(f"[spreadsheet_chunker] Running get_chunks for {self.filename}.")

        # Extract the relevant text from the spreadsheet
        sheets = self._spreadsheet_process()
        logging.info(f"[spreadsheet_chunker] Spreadsheet has {len(sheets)} sheets")

        chunk_id = 0
        for sheet in sheets:
            chunk_id += 1
            chunk_dict = self._create_chunk(chunk_id=chunk_id, content=sheet["table"], summary=sheet["summary"], embedding_text=sheet["summary"], title=sheet["name"]) 
            chunks.append(chunk_dict)    
        return chunks

    def _spreadsheet_process(self):
        blob_data = self.blob_client.download_blob(self.file_url)
        blob_stream = BytesIO(blob_data)
        workbook = load_workbook(blob_stream, data_only=True)
        # Process each sheet in the workbook
        sheets = []
        
        for sheet_name in workbook.sheetnames:
            sheet_dict = {}            
            sheet_dict['name'] = sheet_name
            sheet = workbook[sheet_name]
            
            table = self._excel_to_html(sheet)
            prompt = f"Summarize the html table provided.\ntable_content: \n{table} "
            summary = self.aoai_client.get_completion(prompt, max_tokens=4096)
            sheet_dict["summary"] = summary
              
            if self.token_estimator.estimate_tokens(table) < self.max_chunk_size:
                sheet_dict["table"] = table
            else:
                table = self._excel_to_markdown(sheet)
                if self.token_estimator.estimate_tokens(table) < self.max_chunk_size:
                    sheet_dict["table"] = table
                else:
                    sheet_dict["table"] = summary
            sheets.append(sheet_dict)
        
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

