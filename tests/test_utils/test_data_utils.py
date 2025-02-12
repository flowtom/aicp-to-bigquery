# test_data_utils.py
import unittest
from datetime import datetime
from budget_sync.utils.data_utils import safe_float_convert, process_budget_row

class TestDataUtils(unittest.TestCase):
    def setUp(self):
        self.timestamp = datetime.now()
        self.upload_id = "test-upload-id"
        self.class_code = "A"
        self.class_name = "PREPRODUCTION & WRAP CREW"

    def test_safe_float_convert(self):
        """Test float conversion with various inputs"""
        test_cases = [
            ("123", 123.0),
            ("123.45", 123.45),
            ("1,234.56", 1234.56),
            ("#N/A", None),
            ("N/A", None),
            ("", None),
            ("Total", None),
            ("abc", None),
            (None, None),
            (123, 123.0),
            (123.45, 123.45)
        ]
        
        for input_val, expected in test_cases:
            with self.subTest(input_val=input_val):
                result = safe_float_convert(input_val)
                self.assertEqual(result, expected)

    def test_process_budget_row(self):
        """Test budget row processing"""
        test_cases = [
            # Valid line item
            (
                ["1", "Line Producer", "", "5", "1400", "7000"],
                {
                    'upload_id': self.upload_id,
                    'budget_name': 'Test Budget',
                    'upload_timestamp': self.timestamp,
                    'class_code': self.class_code,
                    'class_name': self.class_name,
                    'line_item_number': 1,
                    'line_item_description': "Line Producer",
                    'estimate_days': 5.0,
                    'estimate_rate': 1400.0,
                    'estimate_total': 7000.0
                }
            ),
            # Header row should return None
            (["ESTIMATE", "", "", "", "", ""], None),
            # Empty row should return None
            (["", "", "", "", "", ""], None),
            # Total row should return None
            (["Total A,C", "", "", "", "", ""], None),
            # Row with #N/A values
            (
                ["2", "Assistant Director", "", "5", "#N/A", "0"],
                {
                    'upload_id': self.upload_id,
                    'budget_name': 'Test Budget',
                    'upload_timestamp': self.timestamp,
                    'class_code': self.class_code,
                    'class_name': self.class_name,
                    'line_item_number': 2,
                    'line_item_description': "Assistant Director",
                    'estimate_days': 5.0,
                    'estimate_rate': None,
                    'estimate_total': 0.0
                }
            )
        ]
        
        for input_row, expected in test_cases:
            with self.subTest(input_row=input_row):
                result = process_budget_row(
                    input_row, 
                    self.class_code, 
                    self.class_name, 
                    self.upload_id, 
                    self.timestamp
                )
                if expected is None:
                    self.assertIsNone(result)
                else:
                    # Compare everything except timestamp (which will be different)
                    result_no_timestamp = {k: v for k, v in result.items() if k != 'upload_timestamp'}
                    expected_no_timestamp = {k: v for k, v in expected.items() if k != 'upload_timestamp'}
                    self.assertEqual(result_no_timestamp, expected_no_timestamp)

if __name__ == '__main__':
    unittest.main()