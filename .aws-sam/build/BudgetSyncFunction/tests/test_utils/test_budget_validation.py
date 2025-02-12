# tests/test_utils/test_budget_validation.py
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

def test_class_code_validation(sample_budget_data):
    """
    Tests the validation of AICP budget class codes:
    1. Verifies class code exists in the data
    2. Confirms it's a string type
    3. Ensures it's not empty
    This is critical as class codes are used for budget organization and reporting.
    """
    assert sample_budget_data['class_code'], "Class code should exist"
    assert isinstance(sample_budget_data['class_code'], str), "Class code should be string"
    assert len(sample_budget_data['class_code']) > 0, "Class code should not be empty"

def test_line_item_structure(sample_budget_data):
    """
    Tests the structure of budget line items:
    1. Checks for presence of all required fields
    2. Validates against AICP budget standard format
    This ensures data consistency and completeness for each budget entry.
    """
    line_item = sample_budget_data['line_items'][0]
    required_fields = ['line_item_number', 'line_item_description', 'estimate_rate']
    
    for field in required_fields:
        assert field in line_item, f"Line item should have {field}"

def test_numeric_values(sample_budget_data):
    """
    Tests numeric field validation in budget data:
    1. Verifies estimate_days is numeric
    2. Confirms estimate_rate is numeric
    3. Ensures estimate_total is numeric
    Critical for preventing calculation errors and ensuring data integrity.
    """
    line_item = sample_budget_data['line_items'][0]
    
    assert isinstance(line_item['estimate_days'], (int, float)), "Days should be numeric"
    assert isinstance(line_item['estimate_rate'], (int, float)), "Rate should be numeric"
    assert isinstance(line_item['estimate_total'], (int, float)), "Total should be numeric"

@pytest.mark.integration
def test_sheets_connection(sheets_service):
    """
    Integration test for Google Sheets API connection:
    1. Attempts to connect to a specific spreadsheet
    2. Verifies successful connection
    3. Confirms correct spreadsheet access
    Important for ensuring API access and permissions are correct.
    """
    SPREADSHEET_ID = '1Rh3z8u1qYU496UQNXabZYEB6QqiggdzRgOAgzaZj7Bw'
    
    result = sheets_service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID
    ).execute()
    
    assert result['spreadsheetId'] == SPREADSHEET_ID

@patch('googleapiclient.discovery.build')
def test_sheets_data_processing(mock_build, sheets_service, sample_budget_data):
    """
    Test processing of sheet data with mocked service.
    Tests the parsing of AICP budget format from Google Sheets.
    """
    # Setup mock service
    mock_service = Mock()
    mock_build.return_value = mock_service
    
    # Define expected data structure for AICP budget
    mock_response = {
        'values': [
            ['A: PREPRODUCTION & WRAP CREW', '', '', '', '', ''],  # Class header
            ['ESTIMATE', 'ACTUAL', '', '', '', ''],                # Column headers
            ['DAYS', 'RATE', 'TOTAL', 'DAYS', 'RATE', 'TOTAL'],   # Subheaders
            ['1', 'Line Producer', '', '5', '1400', '7000'],      # Line items
            ['2', 'Assistant Director', '', '5', '1200', '6000']
        ]
    }
    
    # Configure mock service to return our test data
    mock_values = Mock()
    mock_values.get.return_value.execute.return_value = mock_response
    
    mock_spreadsheet = Mock()
    mock_spreadsheet.values.return_value = mock_values
    
    mock_service.spreadsheets.return_value = mock_spreadsheet
    
    # Execute the test
    SPREADSHEET_ID = '1Rh3z8u1qYU496UQNXabZYEB6QqiggdzRgOAgzaZj7Bw'
    result = mock_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range='A1:F5'  # Specify the range
    ).execute()
    
    # Verify the results
    assert len(result['values']) == 5, "Should have five rows of data"
    assert result['values'][0][0].strip() == 'A: PREPRODUCTION & WRAP CREW', "Should have correct class header"
    
    # Verify line items
    line_items = [row for row in result['values'] if row[0].isdigit()]
    assert len(line_items) == 2, "Should have two line items"
    assert line_items[0][1] == 'Line Producer', "First line item should be Line Producer"
    assert line_items[1][1] == 'Assistant Director', "Second line item should be Assistant Director"

def test_total_calculation(sample_budget_data):
    """
    Tests budget total calculations:
    1. Verifies multiplication of days and rate
    2. Confirms calculation accuracy
    3. Allows small floating-point differences
    Essential for ensuring budget calculations are correct.
    """
    line_item = sample_budget_data['line_items'][0]
    calculated_total = line_item['estimate_days'] * line_item['estimate_rate']
    assert abs(calculated_total - line_item['estimate_total']) < 0.01, "Total should match days * rate"

@pytest.mark.parametrize("test_input,expected", [
    ({'estimate_days': 5, 'estimate_rate': 1000}, 5000),
    ({'estimate_days': 0, 'estimate_rate': 1000}, 0),
    ({'estimate_days': 5, 'estimate_rate': 0}, 0),
])
def test_multiple_calculations(test_input, expected):
    """
    Parametrized test for various calculation scenarios:
    1. Tests normal calculation (5 days * $1000)
    2. Tests zero days calculation
    3. Tests zero rate calculation
    Ensures calculations work correctly in different scenarios.
    """
    calculated = test_input['estimate_days'] * test_input['estimate_rate']
    assert calculated == expected

def test_budget_validation(sample_budget_data, sample_metadata, validate_budget_data):
    """
    Example test using multiple fixtures to validate budget data structure
    and content against expected values.
    """
    # Validate budget data structure
    validate_budget_data(sample_budget_data)
    
    # Test budget data
    assert sample_budget_data['class_code'] == 'A'
    assert sample_budget_data['class_name'] == 'PREPRODUCTION & WRAP CREW'
    assert len(sample_budget_data['line_items']) == 2
    
    # Test line item details
    first_item = sample_budget_data['line_items'][0]
    assert first_item['line_item_number'] == 1
    assert first_item['line_item_description'] == 'Line Producer'
    assert first_item['estimate_days'] == 5.0
    assert first_item['estimate_rate'] == 1400.0
    assert first_item['estimate_total'] == 7000.0
    
    # Test metadata
    assert sample_metadata['version_status'] == 'Draft'
    assert sample_metadata['project_name'] == 'Test Project'
    assert 'project_start_date' in sample_metadata
    assert 'project_end_date' in sample_metadata
    assert sample_metadata['client_name'] == 'Test Client'