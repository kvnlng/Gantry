import unittest
from unittest.mock import patch, MagicMock
import numpy as np
from gantry.entities import Instance
from gantry import pixel_analysis

class TestPixelAnalysis(unittest.TestCase):
    
    @patch('gantry.pixel_analysis.HAS_OCR', True)
    @patch('gantry.pixel_analysis.pytesseract')
    def test_detect_text_simple(self, mock_pytesseract):
        # Setup
        mock_pytesseract.image_to_string.return_value = "DETECTED TEXT"
        
        # Test 2D Array
        pixel_data = np.zeros((100, 100), dtype=np.uint8)
        
        result = pixel_analysis.detect_text(pixel_data)
        
        self.assertEqual(result, "DETECTED TEXT")
        mock_pytesseract.image_to_string.assert_called_once()
    
    @patch('gantry.pixel_analysis.HAS_OCR', True)
    @patch('gantry.pixel_analysis.pytesseract')
    def test_analyze_pixels_integration(self, mock_pytesseract):
        # Setup
        mock_pytesseract.image_to_string.return_value = "  SECRET  "
        
        instance = MagicMock(spec=Instance)
        instance.sop_instance_uid = "1.2.3.4"
        # Return a fake image
        instance.get_pixel_data.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        
        # Run
        findings = pixel_analysis.analyze_pixels(instance)
        
        self.assertEqual(len(findings), 1)
        self.assertEqual(findings[0].value, "SECRET")
        self.assertEqual(findings[0].entity_uid, "1.2.3.4")

    @patch('gantry.pixel_analysis.HAS_OCR', False)
    def test_graceful_degradation(self):
        instance = MagicMock(spec=Instance)
        instance.get_pixel_data.return_value = np.zeros((100, 100), dtype=np.uint8)
        
        findings = pixel_analysis.analyze_pixels(instance)
        self.assertEqual(len(findings), 0)

if __name__ == '__main__':
    unittest.main()
