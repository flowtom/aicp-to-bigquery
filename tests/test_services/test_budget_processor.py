"""
Tests for the budget processor service.
"""
import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from budget_sync.services.budget_processor import BudgetProcessor

@pytest.fixture
def mock_sheets_data():
    """Fixture providing test sheet data."""
    return {
        'values': [
            ['A: PREPRODUCTION & WRAP CREW', '', '', '', '', ''],
            ['ESTIMATE', 'ACTUAL', '', '', '', ''],
            ['DAYS', 'RATE', 'TOTAL', 'DAYS', 'RATE', 'TOTAL'],
            ['1', 'Line Producer', '', '5', '1,400', '7000'],
            ['2', 'Assistant Director', '', '5', '1,200', '6000'],
            ['', '', '', '', '', ''],
            ['B: PRODUCTION STAFF', '', '', '', '', ''],
            ['3', 'Production Manager', '', '3', '800', '2400']
        ]
    }

@pytest.fixture
def processor():
    """Fixture providing a BudgetProcessor instance with mocked credentials."""
    with patch('google.oauth2.service_account.Credentials.from_service_account_file'):
        with patch('googleapiclient.discovery.build'):
            return BudgetProcessor('dummy_credentials.json')

def test_budget_processing_flow(processor, mock_sheets_data):
    """Test the entire budget processing flow."""
    with patch.object(processor.sheets_service.spreadsheets().values(), 'get') as mock_get:
        # Setup mock response
        mock_get.return_value.execute.return_value = mock_sheets_data
        
        # Process sheet
        rows, metadata = processor.process_sheet('dummy_id', 'Sheet1!A1:F100')
        
        # Verify results
        assert len(rows) == 3, "Should process three line items"
        
        # Verify first line item
        first_item = rows[0]
        assert first_item['class_code'] == 'A'
        assert first_item['line_item_description'] == 'Line Producer'
        assert first_item['estimate_rate'] == 1400.0
        
        # Verify class transition
        assert rows[2]['class_code'] == 'B'
        assert rows[2]['line_item_description'] == 'Production Manager'
        
        # Verify metadata
        assert metadata['processed_rows'] == 3
        assert metadata['total_estimate'] == 15400.0  # Sum of all estimates
        assert metadata['version_status'] == 'Draft'

def test_class_header_parsing(processor):
    """Test parsing of class headers."""
    test_cases = [
        (
            'A: PREPRODUCTION & WRAP CREW',
            {'code': 'A', 'name': 'PREPRODUCTION & WRAP CREW'}
        ),
        (
            'B: PRODUCTION STAFF',
            {'code': 'B', 'name': 'PRODUCTION STAFF'}
        ),
        (
            ' C: POST PRODUCTION ',  # Test with extra spaces
            {'code': 'C', 'name': 'POST PRODUCTION'}
        )
    ]
    
    for header, expected in test_cases:
        result = processor._parse_class_header(header)
        assert result == expected

def test_line_item_validation(processor):
    """Test line item validation logic."""
    test_cases = [
        # Valid line items
        ([True, "1", "Description", "", "5", "1000", "5000"]),
        # Invalid cases
        ([False, "", "", "", "", ""]),  # Empty row
        ([False, "ESTIMATE", "", "", "", ""]),  # Header row
        ([False, "Total A,C", "", "", "", ""]),  # Total row
    ]
    
    for expected_valid, *row in test_cases:
        assert processor._is_line_item(row) == expected_valid

def test_process_line_item(processor):
    """Test processing of individual line items."""
    current_class = {'code': 'A', 'name': 'TEST CLASS'}
    upload_id = 'test_upload'
    
    test_cases = [
        # Valid line item
        (
            ['1', 'Test Item', '', '5', '1000', '5000'],
            {
                'line_item_number': 1,
                'line_item_description': 'Test Item',
                'estimate_days': 5.0,
                'estimate_rate': 1000.0,
                'estimate_total': 5000.0
            }
        ),
        # Line item with missing values
        (
            ['2', 'Another Item', '', '', '#N/A', ''],
            {
                'line_item_number': 2,
                'line_item_description': 'Another Item',
                'estimate_days': None,
                'estimate_rate': None,
                'estimate_total': None
            }
        )
    ]
    
    for row, expected_values in test_cases:
        result = processor._process_line_item(row, current_class, upload_id)
        assert result is not None
        
        # Check expected values
        for key, value in expected_values.items():
            assert result[key] == value
        
        # Check required fields
        assert result['upload_id'] == upload_id
        assert result['class_code'] == current_class['code']
        assert result['class_name'] == current_class['name']
        assert 'upload_timestamp' in result
        assert 'budget_name' in result 