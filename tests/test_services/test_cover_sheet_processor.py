import unittest

from src.budget_sync.services import cover_sheet_processor


class DummyExecuteResult:
    def __init__(self, valueRanges):
        self.valueRanges = valueRanges

    def execute(self):
        return {'valueRanges': self.valueRanges}


class DummySpreadsheets:
    def values(self):
        return self

    def batchGet(self, spreadsheetId, ranges):
        # Construct dummy response: for each range, return a value that appends '_dummy' to the range string
        valueRanges = []
        for r in ranges:
            valueRanges.append({'range': r, 'values': [[r + '_dummy']]})
        return DummyExecuteResult(valueRanges)


class DummySheetsService:
    def spreadsheets(self):
        return DummySpreadsheets()


class TestCoverSheetProcessor(unittest.TestCase):
    def test_process_cover_sheet_structure(self):
        dummy_service = DummySheetsService()
        spreadsheet_id = 'dummy_id'
        sheet_title = 'Cover_Sheet'
        result = cover_sheet_processor.process_cover_sheet(dummy_service, spreadsheet_id, sheet_title)
        
        # Validate that the structure contains the expected keys
        self.assertIn('project_summary', result, 'Result should contain project_summary')
        self.assertIn('financials', result, 'Result should contain financials')

        project_summary = result['project_summary']
        self.assertIn('project_info', project_summary, 'project_summary should have project_info')
        self.assertIn('core_team', project_summary, 'project_summary should have core_team')
        self.assertIn('timeline', project_summary, 'project_summary should have timeline')

        financials = result['financials']
        self.assertIn('firm_bid', financials, 'financials should have firm_bid')
        self.assertIn('grand_total', financials, 'financials should have grand_total')

    def test_value_formatting(self):
        # Test that _format_money function works as expected
        from src.budget_sync.services import cover_sheet_processor
        self.assertEqual(cover_sheet_processor._format_money('1234.5'), '$1,234.50')
        self.assertEqual(cover_sheet_processor._format_money('$1234.5'), '$1234.5')
        self.assertEqual(cover_sheet_processor._format_money(None), '$0.00')


if __name__ == '__main__':
    unittest.main() 