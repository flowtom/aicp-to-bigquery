import json


def format_cover_sheet_for_bq(processed_data):
    """
    Extracts and formats the Cover Sheet portion from the processed JSON for BigQuery upload.
    Returns a dictionary representing a row for the budgets table.

    Expected JSON structure:
    {
        "upload_id": "unique_budget_identifier",
        "project_summary" or "cover_sheet": {
            "project_info": {"project_title": ..., "production_company": ..., "contact_phone": ..., "date": ...},
            "core_team": {"director": ..., "producer": ..., "writer": ...},
            "timeline": {"pre_prod_days": ..., "build_days": ..., "pre_light_days": ..., "studio_days": ..., "location_days": ..., "wrap_days": ...},
            "financials": {
                "firm_bid": {"estimated": ..., "actual": ...},
                "grand_total": {"estimated": ..., "actual": ...}
            }
        },
        "upload_timestamp": "timestamp",
        "version_status": "draft"
    }
    """
    # Use 'cover_sheet' key if available, otherwise fallback to 'project_summary'
    cover_sheet = processed_data.get('cover_sheet')
    if cover_sheet is None:
        cover_sheet = processed_data.get('project_summary', {})

    project_info = cover_sheet.get('project_info', {})
    core_team = cover_sheet.get('core_team', {})
    timeline = cover_sheet.get('timeline', {})
    financials = cover_sheet.get('financials', {})
    firm_bid = financials.get('firm_bid', {})
    grand_total = financials.get('grand_total', {})

    row = {
        'budget_id': processed_data.get('upload_id', ''),
        'project_title': project_info.get('project_title', ''),
        'production_company': project_info.get('production_company', ''),
        'contact_phone': project_info.get('contact_phone', ''),
        'date': project_info.get('date', ''),
        'director': core_team.get('director', ''),
        'producer': core_team.get('producer', ''),
        'writer': core_team.get('writer', ''),
        'pre_prod_days': timeline.get('pre_prod_days', ''),
        'build_days': timeline.get('build_days', ''),
        'pre_light_days': timeline.get('pre_light_days', ''),
        'studio_days': timeline.get('studio_days', ''),
        'location_days': timeline.get('location_days', ''),
        'wrap_days': timeline.get('wrap_days', ''),
        'firm_bid_estimated': firm_bid.get('estimated', ''),
        'firm_bid_actual': firm_bid.get('actual', ''),
        'grand_total_estimated': grand_total.get('estimated', ''),
        'grand_total_actual': grand_total.get('actual', ''),
        'upload_timestamp': processed_data.get('upload_timestamp', ''),
        'version_status': processed_data.get('version_status', '')
    }
    return row


def format_line_items_for_bq(processed_data):
    """
    Extracts and formats line items from the processed JSON for BigQuery upload.
    Returns a list of dictionaries, each representing a row for the budget_details table.

    Expected each line item has keys like:
      'class_code', 'line_item_number', 'line_item_description',
      'estimate_days', 'estimate_rate', 'estimate_total', 'actual_total',
      'validation_status', and 'validation_messages'
    """
    budget_id = processed_data.get('upload_id', '')
    line_items = processed_data.get('line_items', [])
    formatted_items = []

    for item in line_items:
        row = {
            'budget_id': budget_id,
            'class_code': item.get('class_code', ''),
            'line_item_number': item.get('line_item_number', ''),
            'line_item_description': item.get('line_item_description', ''),
            'estimate_days': item.get('estimate_days', ''),
            'estimate_rate': item.get('estimate_rate', ''),
            'estimate_total': item.get('estimate_total', ''),
            'actual_total': item.get('actual_total', ''),
            'validation_status': item.get('validation_status', ''),
            'validation_messages': '; '.join(item.get('validation_messages', [])) if isinstance(item.get('validation_messages'), list) else item.get('validation_messages', '')
        }
        formatted_items.append(row)

    return formatted_items 