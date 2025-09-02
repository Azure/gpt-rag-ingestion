import unittest
import jsonschema
from function_app import get_request_schema


class TestSchema(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        self.schema = get_request_schema()
    
    def test_valid_schema(self):
        """Test that valid data passes schema validation."""
        valid_data = {
            "values": [{
                "recordId": "1",
                "data": {
                    "documentUrl": "https://example.com/document.pdf",
                    "documentContentType": "application/pdf",
                    "documentSasToken": "some-token"
                }
            }]
        }
        
        # Should not raise an exception
        jsonschema.validate(valid_data, schema=self.schema)
    
    def test_missing_values_field(self):
        """Test that missing 'values' field fails validation."""
        invalid_data = {
            "data": {
                "documentUrl": "https://example.com/document.pdf",
                "documentContentType": "application/pdf"
            }
        }
        
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(invalid_data, schema=self.schema)
    
    def test_empty_values_array(self):
        """Test that empty values array fails validation."""
        invalid_data = {
            "values": []
        }
        
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(invalid_data, schema=self.schema)
    
    def test_missing_required_data_fields(self):
        """Test that missing required data fields fail validation."""
        # Missing documentUrl
        invalid_data1 = {
            "values": [{
                "recordId": "1",
                "data": {
                    "documentContentType": "application/pdf"
                }
            }]
        }
        
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(invalid_data1, schema=self.schema)
        
        # Missing documentContentType
        invalid_data2 = {
            "values": [{
                "recordId": "1",
                "data": {
                    "documentUrl": "https://example.com/document.pdf"
                }
            }]
        }
        
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(invalid_data2, schema=self.schema)
    
    def test_missing_record_id(self):
        """Test that missing recordId fails validation."""
        invalid_data = {
            "values": [{
                "data": {
                    "documentUrl": "https://example.com/document.pdf",
                    "documentContentType": "application/pdf"
                }
            }]
        }
        
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(invalid_data, schema=self.schema)
    
    def test_empty_document_url(self):
        """Test that empty documentUrl fails validation."""
        invalid_data = {
            "values": [{
                "recordId": "1",
                "data": {
                    "documentUrl": "",
                    "documentContentType": "application/pdf"
                }
            }]
        }
        
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(invalid_data, schema=self.schema)
    
    def test_empty_document_content_type(self):
        """Test that empty documentContentType fails validation."""
        invalid_data = {
            "values": [{
                "recordId": "1",
                "data": {
                    "documentUrl": "https://example.com/document.pdf",
                    "documentContentType": ""
                }
            }]
        }
        
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            jsonschema.validate(invalid_data, schema=self.schema)
    
    def test_optional_sas_token(self):
        """Test that documentSasToken is optional and can be empty."""
        # Without documentSasToken
        valid_data1 = {
            "values": [{
                "recordId": "1",
                "data": {
                    "documentUrl": "https://example.com/document.pdf",
                    "documentContentType": "application/pdf"
                }
            }]
        }
        
        # Should not raise an exception
        jsonschema.validate(valid_data1, schema=self.schema)
        
        # With empty documentSasToken
        valid_data2 = {
            "values": [{
                "recordId": "1",
                "data": {
                    "documentUrl": "https://example.com/document.pdf",
                    "documentContentType": "application/pdf",
                    "documentSasToken": ""
                }
            }]
        }
        
        # Should not raise an exception
        jsonschema.validate(valid_data2, schema=self.schema)


if __name__ == '__main__':
    unittest.main()
