import unittest
from utils.file_utils import get_file_extension, get_filename


class TestFileUtils(unittest.TestCase):
    
    def test_get_file_extension(self):
        """Test file extension extraction."""
        # Test normal file
        self.assertEqual(get_file_extension("document.pdf"), "pdf")
        self.assertEqual(get_file_extension("image.jpg"), "jpg")
        self.assertEqual(get_file_extension("data.xlsx"), "xlsx")
        
        # Test with path
        self.assertEqual(get_file_extension("/path/to/document.pdf"), "pdf")
        self.assertEqual(get_file_extension("folder/subfolder/file.txt"), "txt")
        
        # Test edge cases
        self.assertEqual(get_file_extension("file"), "file")  # No extension
        self.assertEqual(get_file_extension("file."), "")     # Empty extension
        self.assertEqual(get_file_extension(".hidden"), "hidden")  # Hidden file
        self.assertEqual(get_file_extension("file.tar.gz"), "gz")  # Multiple extensions
    
    def test_get_filename(self):
        """Test filename extraction from document URLs."""
        # Test with documents/ path
        self.assertEqual(get_filename("https://storage.com/documents/file.pdf"), "file.pdf")
        self.assertEqual(get_filename("https://storage.com/documents/folder/file.pdf"), "folder/file.pdf")
        
        # Test without documents/ path
        self.assertEqual(get_filename("https://storage.com/other/file.pdf"), "")
        
        # Test edge cases
        self.assertEqual(get_filename("https://storage.com/documents/"), "")
        self.assertEqual(get_filename("documents/file.pdf"), "file.pdf")
        self.assertEqual(get_filename(""), "")


if __name__ == '__main__':
    unittest.main()
