import pytest
from unittest.mock import Mock, AsyncMock, patch
import azure.functions as func
from function_app import document_chunking


@pytest.mark.asyncio
async def test_api_schema_error():
    """Test that the API returns 400 for schema validation errors."""
    
    # Create a mock request with invalid JSON body (missing required fields)
    req = Mock(spec=func.HttpRequest)
    req.get_json.return_value = {
        "data": {
            "content": "This is a test document.",
            "documentUrl": "https://example.com/test-document.txt",
        }
    }
    
    # Call the async function
    response = await document_chunking(req)
    
    # Check the response
    assert response.status_code == 400


@pytest.mark.asyncio 
async def test_api_valid_request():
    """Test that the API processes valid requests correctly."""
    
    # Create a mock request with valid JSON body
    req = Mock(spec=func.HttpRequest)
    req.get_json.return_value = {
        "values": [{
            "recordId": "1",
            "data": {
                "documentUrl": "https://example.com/test.pdf",
                "documentContentType": "application/pdf",
                "documentSasToken": ""
            }
        }]
    }
    
    # Mock dependencies
    with patch('function_app.BlobStorageClient') as mock_blob_client, \
         patch('function_app.DocumentChunker') as mock_chunker:
        
        # Setup mocks
        mock_blob_instance = mock_blob_client.return_value
        mock_blob_instance.download_blob.return_value = b"test content"
        
        mock_chunker_instance = mock_chunker.return_value
        mock_chunker_instance.chunk_documents = AsyncMock(return_value=([], [], []))
        
        # Call the async function
        response = await document_chunking(req)
        
        # Check the response
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_api_empty_body():
    """Test that the API handles empty request body."""
    
    # Create a mock request with no JSON body
    req = Mock(spec=func.HttpRequest)
    req.get_json.return_value = None
    
    # Call the async function
    response = await document_chunking(req)
    
    # Check the response
    assert response.status_code == 400
