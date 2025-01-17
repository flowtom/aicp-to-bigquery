"""
Shared test fixtures for the budget sync package.
"""
import pytest
from datetime import datetime
from typing import Dict, Any

@pytest.fixture
def sample_budget_data() -> Dict[str, Any]:
    """
    Fixture providing sample budget data for testing.
    This represents a typical AICP budget structure.
    """
    return {
        'class_code': 'A',
        'class_name': 'PREPRODUCTION & WRAP CREW',
        'line_items': [
            {
                'line_item_number': 1,
                'line_item_description': 'Line Producer',
                'estimate_days': 5.0,
                'estimate_rate': 1400.0,
                'estimate_total': 7000.0,
                'actual_days': None,
                'actual_rate': None,
                'actual_total': None
            },
            {
                'line_item_number': 2,
                'line_item_description': 'Assistant Director',
                'estimate_days': 5.0,
                'estimate_rate': 1200.0,
                'estimate_total': 6000.0,
                'actual_days': None,
                'actual_rate': None,
                'actual_total': None
            }
        ]
    }

@pytest.fixture
def sample_metadata() -> Dict[str, Any]:
    """
    Fixture providing sample metadata for testing.
    This represents typical budget metadata.
    """
    return {
        'version_status': 'Draft',
        'project_name': 'Test Project',
        'project_start_date': datetime.now().date().isoformat(),
        'project_end_date': datetime.now().date().isoformat(),
        'client_name': 'Test Client',
        'producer_name': 'Test Producer',
        'user_email': 'test@example.com',
        'version_notes': 'Initial version'
    }

@pytest.fixture
def validate_budget_data():
    """
    Fixture providing a validation function for budget data.
    This helps ensure consistent data structure across tests.
    """
    def _validate(data: Dict[str, Any]) -> None:
        """Validate budget data structure."""
        assert 'class_code' in data
        assert 'class_name' in data
        assert 'line_items' in data
        assert isinstance(data['line_items'], list)
        
        for item in data['line_items']:
            assert 'line_item_number' in item
            assert 'line_item_description' in item
            assert 'estimate_days' in item
            assert 'estimate_rate' in item
            assert 'estimate_total' in item
            
            if item['estimate_days'] and item['estimate_rate']:
                calculated = item['estimate_days'] * item['estimate_rate']
                assert abs(calculated - item['estimate_total']) < 0.01
    
    return _validate