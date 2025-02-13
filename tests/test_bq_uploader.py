import unittest

from src.budget_sync.services.bq_uploader import format_cover_sheet_for_bq, format_line_items_for_bq


class TestBQUploader(unittest.TestCase):

    def setUp(self):
        # Sample processed JSON data
        self.sample_processed_data = {
            "upload_id": "budget_123",
            "cover_sheet": {
                "project_info": {
                    "project_title": "Test Project",
                    "production_company": "Test Studios",
                    "contact_phone": "123-456-7890",
                    "date": "2024-01-01"
                },
                "core_team": {
                    "director": "Director Name",
                    "producer": "Producer Name",
                    "writer": "Writer Name"
                },
                "timeline": {
                    "pre_prod_days": "5",
                    "build_days": "10",
                    "pre_light_days": "3",
                    "studio_days": "7",
                    "location_days": "4",
                    "wrap_days": "2"
                },
                "financials": {
                    "firm_bid": {
                        "estimated": "$100,000.00",
                        "actual": "$95,000.00"
                    },
                    "grand_total": {
                        "estimated": "$150,000.00",
                        "actual": "$145,000.00"
                    }
                }
            },
            "line_items": [
                {
                    "class_code": "A",
                    "line_item_number": "1",
                    "line_item_description": "Expense 1",
                    "estimate_days": "2",
                    "estimate_rate": "$500.00",
                    "estimate_total": "$1,000.00",
                    "actual_total": "$1,000.00",
                    "validation_status": "valid",
                    "validation_messages": ["No issues"]
                },
                {
                    "class_code": "B",
                    "line_item_number": "2",
                    "line_item_description": "Expense 2",
                    "estimate_days": "3",
                    "estimate_rate": "$600.00",
                    "estimate_total": "$1,800.00",
                    "actual_total": "$1,800.00",
                    "validation_status": "warning",
                    "validation_messages": []
                }
            ],
            "upload_timestamp": "2024-01-22T15:30:45.123456",
            "version_status": "draft"
        }

    def test_format_cover_sheet_for_bq(self):
        # Expected dictionary for budgets table
        expected_output = {
            'budget_id': "budget_123",
            'project_title': "Test Project",
            'production_company': "Test Studios",
            'contact_phone': "123-456-7890",
            'date': "2024-01-01",
            'director': "Director Name",
            'producer': "Producer Name",
            'writer': "Writer Name",
            'pre_prod_days': "5",
            'build_days': "10",
            'pre_light_days': "3",
            'studio_days': "7",
            'location_days': "4",
            'wrap_days': "2",
            'firm_bid_estimated': "$100,000.00",
            'firm_bid_actual': "$95,000.00",
            'grand_total_estimated': "$150,000.00",
            'grand_total_actual': "$145,000.00",
            'upload_timestamp': "2024-01-22T15:30:45.123456",
            'version_status': "draft"
        }
        result = format_cover_sheet_for_bq(self.sample_processed_data)
        self.assertEqual(result, expected_output)

    def test_format_line_items_for_bq(self):
        expected_output = [
            {
                'budget_id': "budget_123",
                'class_code': "A",
                'line_item_number': "1",
                'line_item_description': "Expense 1",
                'estimate_days': "2",
                'estimate_rate': "$500.00",
                'estimate_total': "$1,000.00",
                'actual_total': "$1,000.00",
                'validation_status': "valid",
                'validation_messages': "No issues"
            },
            {
                'budget_id': "budget_123",
                'class_code': "B",
                'line_item_number': "2",
                'line_item_description': "Expense 2",
                'estimate_days': "3",
                'estimate_rate': "$600.00",
                'estimate_total': "$1,800.00",
                'actual_total': "$1,800.00",
                'validation_status': "warning",
                'validation_messages': ""
            }
        ]
        result = format_line_items_for_bq(self.sample_processed_data)
        self.assertEqual(result, expected_output)


if __name__ == '__main__':
    unittest.main() 