import os
import dotenv
import html
import re
import shutil
import time
from azure.core.exceptions import ServiceRequestError
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient  
from azure.search.documents.models import Vector
from azure.ai.formrecognizer import DocumentAnalysisClient
from glob import glob

# load environment variables from .env file
dotenv.load_dotenv()

# parameters
AZURE_SEARCH_SERVICE = os.environ.get('AZURE_SEARCH_SERVICE')
AZURE_SEARCH_KEY = os.environ.get('AZURE_SEARCH_KEY')
AZURE_SEARCH_INDEX = os.environ.get('AZURE_SEARCH_INDEX')
ANALYZER_NAME=os.environ.get('ANALYZER_NAME')

AZURE_STORAGE_ACCOUNT = os.environ.get('AZURE_STORAGE_ACCOUNT')
AZURE_STORAGE_KEY = os.environ.get('AZURE_STORAGE_KEY')
AZURE_STORAGE_CONTAINER = os.environ.get('AZURE_STORAGE_CONTAINER')

AZURE_FORM_REC_SERVICE=os.environ.get('AZURE_FORM_REC_SERVICE')
AZURE_FORM_REC_KEY=os.environ.get('AZURE_FORM_REC_KEY')

INPUT_FOLDER=os.environ.get('INPUT_FOLDER')
WORK_FOLDER='work'
SKIP_BLOBS=True if os.environ.get('SKIP_BLOBS').lower() == 'true' else False
VECTOR_INDEX=True if os.environ.get('VECTOR_INDEX').lower() == 'true' else False
MAX_SECTION_LENGTH=int(os.environ.get('MAX_SECTION_LENGTH'))
SECTION_OVERLAP=int(os.environ.get('SECTION_OVERLAP'))

VERBOSE = True

def blob_name_from_file_page(filename, page):
    return os.path.splitext(os.path.basename(filename))[0] + f"-{page}" + ".pdf"

def upload_blob(filename):
    blob_service = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=AZURE_STORAGE_KEY)
    blob_container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    if not blob_container.exists():
        blob_container.create_container(public_access="container")
    
    if VERBOSE: print(f"[INFO]    Uploading {filename}")
    with open(filename, "rb") as data:
        blob_name = filename.split('/')[-1]
        if blob_name.endswith(".pdf"):
            content_type = "application/pdf"
        elif blob_name.endswith(".png"):
            content_type = "image/png"
        else: # todo: add more types
            content_type = "application/octet-stream"
        my_content_settings = ContentSettings(content_type=content_type)
        blob_container.upload_blob(blob_name, data, overwrite=True, content_settings=my_content_settings)

def remove_blob(filename):
    if VERBOSE: print(f"[INFO]    Removing blob '{filename}'")
    blob_service = BlobServiceClient(account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net", credential=AZURE_STORAGE_KEY)
    blob_container = blob_service.get_container_client(AZURE_STORAGE_CONTAINER)
    if blob_container.exists():
        blobs = blob_container.list_blob_names(name_starts_with=filename)
        for b in blobs:
            if VERBOSE: print(f"[INFO]    Removing blob {b}")
            blob_container.delete_blob(b)

def table_to_html(table):
    table_html = "<table>"
    rows = [sorted([cell for cell in table.cells if cell.row_index == i], key=lambda cell: cell.column_index) for i in range(table.row_count)]
    for row_cells in rows:
        table_html += "<tr>"
        for cell in row_cells:
            tag = "th" if (cell.kind == "columnHeader" or cell.kind == "rowHeader") else "td"
            cell_spans = ""
            if cell.column_span > 1: cell_spans += f" colSpan={cell.column_span}"
            if cell.row_span > 1: cell_spans += f" rowSpan={cell.row_span}"
            table_html += f"<{tag}{cell_spans}>{html.escape(cell.content)}</{tag}>"
        table_html +="</tr>"
    table_html += "</table>"
    return table_html

def in_a_table(paragraph, tables):
    for table in tables:
        for cell in table.cells:
            if len(cell.spans) > 0 and paragraph.spans[0].offset == cell.spans[0].offset:
                return True
    return False

def split_text(files):

    SENTENCE_ENDINGS = [".", "!", "?"]
    WORDS_BREAKS = [",", ";", ":", " ", "(", ")", "[", "]", "{", "}", "\t", "\n"]
    if VERBOSE: print(f"[INFO]    Splitting '{filename}' into chunks")

    # make temporary directory
    if not os.path.exists("./temp"):
        os.mkdir("./temp")

    for i, pdf_path in enumerate(files):
        pdf_filename = pdf_path.split('/')[-1]

        formrec_creds = AzureKeyCredential(AZURE_FORM_REC_KEY)
        endpoint = f"https://{AZURE_FORM_REC_SERVICE}.cognitiveservices.azure.com/"
        document_analysis_client = DocumentAnalysisClient(
            endpoint=endpoint, credential=formrec_creds
        )

        with open(pdf_path, "rb") as f:
            poller = document_analysis_client.begin_analyze_document(
                "prebuilt-layout", document=f
            )
        document = poller.result()

        # tables
        for table in document.tables:
            html_table = table_to_html(table)
            yield(html_table)

        # paragraphs
        section = ""
        for paragraph in document.paragraphs:
            if not in_a_table(paragraph, document.tables):
                if (len(section) + len(paragraph.content)) < MAX_SECTION_LENGTH:
                    section = section + "\n" + paragraph.content
                else:
                    yield(section)
                    section = paragraph.content
        yield(section) # last section

def create_chunks(file_path):
    prefix = file_path[:-4]
    for i, section in enumerate(split_text([file_path])):
        yield {
            "id": f"{prefix}-chk{i}".split('/')[-1],
            "content": section,
            "title": file_path.split('/')[-1].split('.')[0],
            "url": f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net/{AZURE_STORAGE_CONTAINER}/{file_path}",            
            "filepath": file_path.split('/')[-1],
            "chunk_id": str(i)
        }

def create_search_index():
    if VERBOSE: print(f"[INFO] Ensuring search index {AZURE_SEARCH_INDEX} exists")
    search_creds = AzureKeyCredential(AZURE_SEARCH_KEY)
    index_client = SearchIndexClient(endpoint=f"https://{AZURE_SEARCH_SERVICE}.search.windows.net/",
                                     credential=search_creds)
    if AZURE_SEARCH_INDEX not in index_client.list_index_names():
        if VECTOR_INDEX:
           index = SearchIndex(
               name=AZURE_SEARCH_INDEX,
               fields=[
                    SimpleField(name="id", type="Edm.String", key=True),
                    SearchableField(name="content", type="Edm.String", analyzer_name=ANALYZER_NAME),
                    SimpleField(name="title", type="Edm.String", searchable=True, retrievable=True),
                    SimpleField(name="url", type="Edm.String"), 
                    SimpleField(name="filepath", type="Edm.String", filterable=True),
                    SimpleField(name="chunk_id", type="Edm.String"),
                    SearchField(name="titleVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                                searchable=True, dimensions=1536, vector_search_configuration="vector-config"),
                    SearchField(name="contentVector", type=SearchFieldDataType.Collection(SearchFieldDataType.Single),
                                searchable=True, dimensions=1536, vector_search_configuration="vector-config")
                ],
                vector_search = VectorSearch(
                    algorithm_configurations=[
                        VectorSearchAlgorithmConfiguration(
                            name="vector-config",
                            kind="hnsw",
                            hnsw_parameters={
                                "m": 4,
                                "efConstruction": 400,
                                "efSearch": 1000,
                                "metric": "cosine"
                            }
                        )
                    ]
                ),
               semantic_settings=SemanticSettings(
                    configurations=[SemanticConfiguration(
                                    name='default',
                                    prioritized_fields=PrioritizedFields(
                                    title_field=None, prioritized_content_fields=[SemanticField(field_name='content')]))])
            )
        else:
            index = SearchIndex(
                name=AZURE_SEARCH_INDEX,
                fields=[
                    SimpleField(name="id", type="Edm.String", key=True),
                    SearchableField(name="content", type="Edm.String", analyzer_name=ANALYZER_NAME),
                    SimpleField(name="title", type="Edm.String", searchable=True, retrievable=True),
                    SimpleField(name="url", type="Edm.String"), 
                    SimpleField(name="filepath", type="Edm.String", filterable=True),
                    SimpleField(name="chunk_id", type="Edm.String")                
                ],
                semantic_settings=SemanticSettings(
                    configurations=[SemanticConfiguration(
                        name='default',
                        prioritized_fields=PrioritizedFields(
                            title_field=None, prioritized_content_fields=[SemanticField(field_name='content')]))])
            )


        if VERBOSE: print(f"[INFO]    Creating {AZURE_SEARCH_INDEX} search index")
        index_client.create_index(index)
    else:
        if VERBOSE: print(f"[INFO]     Search index {AZURE_SEARCH_INDEX} already exists")

def index_chunks(filename, chunks):
    if VERBOSE: print(f"[INFO]    Indexing chunks from '{filename}' into search index '{AZURE_SEARCH_INDEX}'")
    search_creds = AzureKeyCredential(AZURE_SEARCH_KEY)
    search_client = SearchClient(endpoint=f"https://{AZURE_SEARCH_SERVICE}.search.windows.net/",
                                    index_name=AZURE_SEARCH_INDEX,
                                    credential=search_creds)
    i = 0
    batch = []
    for s in chunks:
        batch.append(s)
        i += 1
        if i % 1000 == 0:
            print("indexing batch") 
            results = search_client.index_documents(batch=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            if VERBOSE: print(f"[INFO]    Indexed {len(results)} chunks, {succeeded} succeeded")
            batch = []

    if len(batch) > 0:
        try:
            results = search_client.upload_documents(documents=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            if VERBOSE: print(f"[INFO]    Indexed {len(results)} chunks, {succeeded} succeeded")            
        except azure.core.exceptions.ServiceRequestError as e:
            print(f"[ERROR]    Error. Retrying in 15 sec... {e}")
            time.sleep(15)
            try: 
                results = search_client.upload_documents(documents=batch)
                succeeded = sum([1 for r in results if r.succeeded])
                if VERBOSE: print(f"[INFO]    Indexed {len(results)} chunks, {succeeded} succeeded")
            except Exception as e:
                print(f"[ERROR]    Error.{e}")


def remove_from_index(filename):
    if VERBOSE: print(f"[INFO]    Removing chunks from '{filename}' from search index '{AZURE_SEARCH_INDEX}'")
    search_creds = AzureKeyCredential(AZURE_SEARCH_KEY)
    search_client = SearchClient(endpoint=f"https://{AZURE_SEARCH_SERVICE}.search.windows.net/",
                                    index_name=AZURE_SEARCH_INDEX,
                                    credential=search_creds)
    while True:
        filter = None if filename == None else f"filepath eq '{filename}'"
        try:
            r = search_client.search("", filter=filter, top=1000, include_total_count=True)
            if r.get_count() == 0:
                break
            r = search_client.delete_documents(documents=[{ "id": d["id"] } for d in r])
            if VERBOSE: print(f"    Removed {len(r)} chunks from index")
        except:
            break
        # It can take a few seconds for search results to reflect changes, so wait a bit
        time.sleep(2)

def add_folder_prefix(files):
    new_files = []
    for file_path in files:
        filename = file_path.split('/')[-1]
        sub_folder = file_path.replace(INPUT_FOLDER, '')[1:-len(filename)]
        sub_folder_prefix = sub_folder.replace('/', '_')
        new_filename = f"{sub_folder_prefix}{filename}"
        new_file_path = os.path.join(WORK_FOLDER, new_filename)
        new_files.append(new_file_path)
        # copy the file to the work folder
        shutil.copy(file_path, new_file_path)
    return new_files

def rename_files_to_url_safe(files):
    new_files = []
    for file_path in files:
        filename = os.path.basename(file_path)
        file_extension = filename.split('.')[-1]
        filename = filename[:-len(file_extension) - 1]
        url_safe_name = re.sub(r'[^a-zA-Z0-9]+', '-', filename) # special chars
        new_file_path = os.path.join(WORK_FOLDER, url_safe_name + '.' + file_extension)
        os.rename(file_path, new_file_path)
        new_files.append(new_file_path)
    return new_files

def create_working_files():
    if os.path.exists(WORK_FOLDER):
        shutil.rmtree(WORK_FOLDER)
    os.makedirs(WORK_FOLDER)
    files = glob(f"{INPUT_FOLDER}/**/*.pdf", recursive=True)
    files = files + glob(f"{INPUT_FOLDER}/**/*.png", recursive=True)
    files = add_folder_prefix(files)
    files = rename_files_to_url_safe(files)
    return files

## main processing

if __name__ == "__main__":
    print(f"[INFO] Start processing...")

    create_search_index()
    
    # save input files in working folder using unique url-safe names
    files = create_working_files()

    for file_path in files:
        filename = os.path.basename(file_path)
        if VERBOSE: print(f"[INFO] Processing '{filename}'")
        remove_from_index(filename)
        if not SKIP_BLOBS:
            remove_blob(filename)            
            upload_blob(file_path)
        chunks = create_chunks(file_path)
        index_chunks(filename, chunks)

    # remove working folder
    shutil.rmtree(WORK_FOLDER)

    print(f"[INFO] Done processing...")            