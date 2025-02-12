from src.budget_sync.services.cover_sheet_processor import CoverSheetProcessor
import logging

# Updated sample grid data for testing based on COVER_SHEET_MAPPINGS
sample_grid_data = {
    'rowData': [
        {'values': [{'formattedValue': 'Sample Project'}, {'formattedValue': 'Sample Company'}, {'formattedValue': '123-456-7890'}, {'formattedValue': '2023-10-01'}]},  # Project Info
        {},  # Empty row for spacing
        {'values': [{'formattedValue': 'Director Name'}, {'formattedValue': 'Producer Name'}, {'formattedValue': 'Writer Name'}]},  # Core Team
        {},  # Empty row for spacing
        {'values': [{'formattedValue': '5'}, {'formattedValue': '10'}, {'formattedValue': '3'}, {'formattedValue': '7'}, {'formattedValue': '2'}, {'formattedValue': '1'}]},  # Timeline
        {},  # Empty row for spacing
        {'values': [{'formattedValue': '1000'}, {'formattedValue': '900'}, {'formattedValue': '100'}, {'formattedValue': '950'}, {'formattedValue': '50'}]},  # Firm Bid Summary
        {},  # Empty row for spacing
        {'values': [{'formattedValue': '5000'}, {'formattedValue': '4500'}, {'formattedValue': '500'}, {'formattedValue': '4700'}, {'formattedValue': '300'}]}  # Grand Total
    ]
}

# Initialize the CoverSheetProcessor with a mock sheets service
sheets_service_mock = None  # Replace with a mock or actual service if needed
processor = CoverSheetProcessor(sheets_service_mock)

# Test the extraction of cover sheet data
try:
    cover_sheet_data = processor.extract_cover_sheet(sample_grid_data)
    print("Extracted Cover Sheet Data:", cover_sheet_data)
except Exception as e:
    logging.error(f"Error during cover sheet extraction: {str(e)}")

# Additional tests can be added here for other methods or scenarios 