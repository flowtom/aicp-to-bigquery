# BigQuery Upload Data Structure

This document outlines the data structure for uploading processed AICP budget data to BigQuery. It explains the expected JSON output, the design of the BigQuery table schemas, and the mapping between JSON keys and table columns.

## Processed JSON Format

The final JSON output from the budget processing consists of distinct sections:

- **cover_sheet / project_summary**: Contains all the Cover Sheet data.
  - Includes fields such as project title, production company, contact phone, date, core team details, timeline milestones, and financial summaries (firm bid and grand total).
  - This section must include a unique `budget_id` that is also present in other sections.

- **line_items**: Contains all the detailed budget line items from classes A through P.
  - Each line item includes details like line item number, description, rates, quantities, totals, etc.
  - Each line item record should have a `budget_id` field to link it back to the main budget record.

- **metadata**: Contains additional processing metadata, such as timestamps, versioning information, and processing statistics.

Example JSON structure:

```json
{
  "budget_id": "unique_budget_identifier",
  "cover_sheet": {
    "project_info": {
      "project_title": "Project X",
      "production_company": "Newfangled Studios",
      "contact_phone": "(123) 456-7890",
      "date": "2024-01-01"
    },
    "core_team": {
      "director": "John Doe",
      "producer": "Jane Smith",
      "writer": "Alice Johnson"
    },
    "timeline": {
      "pre_prod_days": "0",
      "build_days": "0",
      "pre_light_days": "0",
      "studio_days": "0",
      "location_days": "0",
      "wrap_days": "0"
    },
    "financials": {
      "firm_bid": { ... },
      "grand_total": { ... }
    }
  },
  "line_items": [
    {
      "budget_id": "unique_budget_identifier",
      "class_code": "A",
      "line_item_number": "1",
      "line_item_description": "Item description",
      "estimate_days": "5",
      "estimate_rate": "$200.00",
      "estimate_total": "$1,000.00",
      "actual_total": "$1,000.00",
      "validation_status": "valid",
      "validation_messages": []
    },
    ...
  ],
  "metadata": {
    "upload_timestamp": "2024-01-22T15:30:45.123456",
    "version_status": "draft",
    "processing_summary": { ... }
  }
}
```

## BigQuery Table Schemas

### Budgets Table (e.g., `budgets`)

This table stores the Cover Sheet data along with budget metadata.

| Column Name           | Data Type | Description                                         |
|-----------------------|-----------|-----------------------------------------------------|
| budget_id             | STRING    | Unique identifier for the budget                  |
| project_title         | STRING    | Project title from the Cover Sheet                |
| production_company    | STRING    | Production company                                |
| contact_phone         | STRING    | Contact phone number                              |
| date                  | STRING    | Project date (formatted as YYYY-MM-DD)            |
| director              | STRING    | Director name                                     |
| producer              | STRING    | Producer name                                     |
| writer                | STRING    | Writer name                                       |
| pre_prod_days         | STRING    | Pre-production days                               |
| build_days            | STRING    | Build days                                        |
| pre_light_days        | STRING    | Pre-light days                                    |
| studio_days           | STRING    | Studio days                                       |
| location_days         | STRING    | Location days                                     |
| wrap_days             | STRING    | Wrap days                                         |
| firm_bid_estimated    | STRING    | Estimated firm bid (from firm_bid_summary)        |
| firm_bid_actual       | STRING    | Actual firm bid                                   |
| grand_total_estimated | STRING    | Estimated total (grand_total)                     |
| grand_total_actual    | STRING    | Actual total                                      |
| upload_timestamp      | TIMESTAMP | Timestamp when the budget was processed           |
| version_status        | STRING    | Version status (e.g., draft, final)               |

### Budget Details Table (e.g., `budget_details`)

This table stores the detailed line items linked to a budget.

| Column Name           | Data Type | Description                                         |
|-----------------------|-----------|-----------------------------------------------------|
| budget_id             | STRING    | Unique identifier linking to the budgets table      |
| class_code            | STRING    | Budget class code (e.g., A, B, C, ... )               |
| line_item_number      | STRING    | Line item number                                  |
| line_item_description | STRING    | Description of the line item                      |
| estimate_days         | STRING    | Number of days for estimate (if applicable)       |
| estimate_rate         | STRING    | Rate for estimate                                 |
| estimate_total        | STRING    | Total estimated cost                              |
| actual_total          | STRING    | Actual total cost                                 |
| validation_status     | STRING    | Validation status (e.g., valid, warning)          |
| validation_messages   | STRING    | Concatenated validation messages                  |

## Data Mapping Documentation

- **budget_id**: Generated unique identifier present in both the budgets and budget_details tables.
- **cover_sheet (project_summary)**: Mapped directly from the Cover Sheet processed JSON.
  - JSON key: `cover_sheet.project_info.project_title`  -> BigQuery column: `project_title`
  - JSON key: `cover_sheet.project_info.production_company` -> BigQuery column: `production_company`
  - JSON key: `cover_sheet.project_info.contact_phone` -> BigQuery column: `contact_phone`
  - JSON key: `cover_sheet.project_info.date` -> BigQuery column: `date`
  - Similarly map other fields such as team and timeline details.

- **line_items**: Each object in the `line_items` array is inserted as a row in the `budget_details` table.
  - JSON key: `line_items[].class_code` -> BigQuery column: `class_code`
  - JSON key: `line_items[].line_item_number` -> BigQuery column: `line_item_number`
  - JSON key: `line_items[].line_item_description` -> BigQuery column: `line_item_description`
  - Additional mappings for estimates and actuals follow similarly.

## Assumptions and Considerations

- The JSON output from the budget processing must always include a `budget_id` which is used to join the two tables.
- Data types in BigQuery are defined as strings for most financial and text fields, while timestamps are properly formatted for date-time fields.
- Any changes in the structure of the Cover Sheet or line items processing should be reflected by updating this mapping document. 