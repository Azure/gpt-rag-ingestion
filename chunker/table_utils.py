import html
import os
import sys

"""
table_utils.py

This module provides utility functions for handling tables in a document. 

Functions:
- table_to_html(table): Converts a table into HTML format.
- char_in_a_table(offset, tables): Checks if a character at a given offset is in any of the tables.
- paragraph_in_a_table(paragraph, tables): Checks if a paragraph is in any of the tables.
- merge_tables_if_same_structure(tables, pages): Merges tables with the same structure.
- same_structure(table1, table2, pages): Checks if two tables have the same structure.
- merge_tables(table1, table2): Merges two tables.
- text_before_table(document, table, tables): Gets the text before a table in a document.
- text_after_table(document, table, tables): Gets the text after a table in a document.
"""

TOKEN_OVERLAP = int(os.environ["TOKEN_OVERLAP"])
TABLE_DISTANCE_THRESHOLD = 3 # inches

def table_to_html(table):
    table_html = "<table>"
    rows = [sorted([cell for cell in table['cells'] if cell['rowIndex'] == i], key=lambda cell: cell['columnIndex']) for i in range(table['rowCount'])]
    for row_cells in rows:
        table_html += "<tr>"
        for cell in row_cells:
            tag =  "td"
            if 'kind' in cell:
                if (cell['kind'] == "columnHeader" or cell['kind'] == "rowHeader"): tag = "th"
            
            cell_spans = ""
            
            if 'columnSpan' in cell:
                if cell['columnSpan'] > 1: cell_spans += f" colSpan={cell['columnSpan']}"
            
            if 'rowSpan' in cell:
                if cell['rowSpan'] > 1: cell_spans += f" rowSpan={cell['rowSpan']}"

            table_html += f"<{tag}{cell_spans}>{html.escape(cell['content'])}</{tag}>"
        table_html +="</tr>"
    table_html += "</table>"
    return table_html

def char_in_a_table(offset, tables):
    for table in tables:
        for cell in table['cells']:
            if len(cell['spans']) > 0 and cell['spans'][0]['offset'] <= offset < (cell['spans'][0]['offset'] + cell['spans'][0]['length']):
                return True
    return False

def paragraph_in_a_table(paragraph, tables):
    for table in tables:
        for cell in table['cells']:
            if len(cell['spans']) > 0 and paragraph['spans'][0]['offset'] == cell['spans'][0]['offset']:
                return True
    return False

def merge_tables_if_same_structure(tables, pages):
    """
    Merge tables with the same structure. Two tables, table1 and table2, are considered to have the same structure if:
    - They have the same number of columns.
    - The difference in page numbers between the first boundingRegion of table2 and the last boundingRegion of table1 is less than 2.
    - The absolute difference between the last y-coordinate of the first boundingRegion of table1 
      and the first y-coordinate of the first boundingRegion of table2 is less than TABLE_DISTANCE_THRESHOLD.
    """
    merged_tables = []
    for table in tables:
        if not merged_tables or not same_structure(merged_tables[-1], table, pages):
            merged_tables.append(table)
        else:
            merged_tables[-1] = merge_tables(merged_tables[-1], table)
    return merged_tables

def same_structure(table1, table2, pages):
    if table1['columnCount'] != table2['columnCount']:
        return False

    bounding_region1 = table1['boundingRegions'][-1]
    bounding_region2 = table2['boundingRegions'][0]

    page_difference = bounding_region2['pageNumber'] - bounding_region1['pageNumber']

    if page_difference >= 2:
        return False
    
    if page_difference == 1:
        pageIdx = bounding_region1['pageNumber'] - 1
        tables_distance = bounding_region2['polygon'][0] + (pages[pageIdx]['height'] - bounding_region1['polygon'][-1])
    else: 
        tables_distance = bounding_region2['polygon'][0] - bounding_region1['polygon'][-1]

    # tables distance in inches
    if tables_distance >= TABLE_DISTANCE_THRESHOLD:
        return False

    return True

def merge_tables(table1, table2):
    merged_table = table1.copy()
    merged_table['rowCount'] += table2['rowCount']
    table1_row_count = table1['rowCount']
    
    for cell in table2['cells']:
        cell['rowIndex'] += table1_row_count
        merged_table['cells'].append(cell)
    
    merged_table['boundingRegions'].extend(table2['boundingRegions'])
    
    return merged_table

def text_before_table(document, table, tables):
    first_cell_offset = sys.maxsize
    # get first cell
    for cell in table['cells']:
        # Check if the cell has content
        if cell.get('content'):
            # Get the offset of the cell content
            first_cell_offset = cell['spans'][0]['offset'] 
            break
    text_before_offset = max(0,first_cell_offset - TOKEN_OVERLAP)
    text_before_str = ''
    # we don't want to add text before the table if it is contained in a table
    for idx, c in enumerate(document['content'][text_before_offset:first_cell_offset]):
        if not char_in_a_table(text_before_offset+idx, tables):
            text_before_str += c
    return text_before_str

def text_after_table(document, table, tables):
    last_cell_offset = 0
    # get last cell
    for cell in table['cells']:
        # Check if the cell has content
        if cell.get('content'):
            # Get the offset of the cell content
            last_cell_offset = cell['spans'][0]['offset']
    text_after_offset = last_cell_offset + len(cell['content'])
    text_after_str = ''
    # we don't want to add text after the table if it is contained in a table
    for idx, c in enumerate(document['content'][text_after_offset:text_after_offset+TOKEN_OVERLAP]):
        if not char_in_a_table(text_after_offset+idx, tables):
            text_after_str += c
    return text_after_str