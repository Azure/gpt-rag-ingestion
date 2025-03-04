from tools import AISearchClient, BlobStorageClient
from chunking import DocumentChunker
from utils.file_utils import get_filename
import asyncio
from uuid import uuid4

async def upload_to_search(documents, index_name):
    search_client = AISearchClient()
    
    for document in documents:
        await search_client.index_document(
            index_name=index_name,
            document=document
        )
    
    await search_client.close()

def process_document(document_url, content_type, sas_token=""):
    # Prepare input data similar to what the function app expects
    input_data = {
        "recordId": str(uuid4()),
        "data": {
            "documentUrl": document_url,
            "documentContentType": content_type,
            "documentSasToken": sas_token,
            "documentContent": ""  # Can be populated if you have content
        }
    }
    
    # Get document bytes from blob storage
    full_url = f"{document_url}{sas_token}"
    blob_client = BlobStorageClient(full_url)
    document_bytes = blob_client.download_blob()

    # get organization_id from metadata if exists
    metadata = blob_client.get_metadata()

    # get organization_id from metadata if exists
    organization_id = metadata.get("organization_id", "")
    # Add document bytes to input data
    input_data["data"]["documentBytes"] = document_bytes
    input_data["data"]["fileName"] = get_filename(document_url)

    # Use DocumentChunker to chunk the document
    chunks, errors, warnings = DocumentChunker().chunk_documents(input_data["data"])
    
    
    return chunks, errors, warnings, organization_id

# Usage example:
payload = {
    "values": [
        {
            "recordId": "",
            "data": {
                "documentContentType": "application/pdf",
                "documentUrl": ""
                "documentSasToken": ""
                "documentContent": ""
            }
        }
    ]
}

# Process each document in the payload
for item in payload["values"]:
    document_url = item["data"]["documentUrl"]
    content_type = item["data"]["documentContentType"]
    sas_token = item["data"].get("documentSasToken", "")
    
    # Process the document to get chunks
    chunks, errors, warnings, organization_id = process_document(document_url, content_type, sas_token)

    # add organization_id to each chunk
    for chunk in chunks:
        chunk["organization_id"] = organization_id

    if errors:
        print(f"Errors processing {document_url}:", errors)
    if warnings:
        print(f"Warnings processing {document_url}:", warnings)

    # Upload chunks to search index
    if chunks:
        asyncio.run(upload_to_search(chunks, "ragindex"))