class BudgetClass:
    def __init__(self, class_code, class_name, estimate_subtotal=0.0, estimate_pnw=0.0, estimate_total=0.0,
                 actual_subtotal=0.0, actual_pnw=0.0, actual_total=0.0, line_items=None, validation=None):
        self.class_code = class_code
        self.class_name = class_name
        self.estimate_subtotal = estimate_subtotal
        self.estimate_pnw = estimate_pnw
        self.estimate_total = estimate_total
        self.actual_subtotal = actual_subtotal
        self.actual_pnw = actual_pnw
        self.actual_total = actual_total
        self.line_items = line_items or []
        self.validation = validation

    def to_dict(self):
        """Convert BudgetClass object to a dictionary for JSON serialization."""
        return {
            'class_code': self.class_code,
            'class_name': self.class_name,
            'estimate_subtotal': self.estimate_subtotal,
            'estimate_pnw': self.estimate_pnw,
            'estimate_total': self.estimate_total,
            'actual_subtotal': self.actual_subtotal,
            'actual_pnw': self.actual_pnw,
            'actual_total': self.actual_total,
            'line_items': self.line_items,
            'validation': {
                'is_valid': self.validation.is_valid,
                'messages': self.validation.messages
            } if self.validation else None
        }

def lambda_handler(event, context):
    """
    AWS Lambda handler function for processing budget data.
    """
    try:
        # If running in Lambda, copy token.json to /tmp
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            if os.path.exists('token.json'):
                import shutil
                shutil.copy2('token.json', '/tmp/token.json')
                logger.info("Copied token.json to /tmp for Lambda use")

        # Extract parameters from the event
        spreadsheet_id = event.get('spreadsheet_id')
        gid = event.get('gid')

        if not spreadsheet_id or not gid:
            raise ValueError("Missing required parameters: spreadsheet_id and gid")

        # Initialize the processor
        processor = BudgetProcessor()
        
        # Process the budget data
        budget_classes = processor.process_budget(spreadsheet_id, gid)
        
        # Convert budget classes to dictionaries for JSON serialization
        budget_classes_dict = {code: budget_class.to_dict() for code, budget_class in budget_classes.items()}
        
        # Return the processed data
        return {
            'statusCode': 200,
            'body': {
                'budget_classes': budget_classes_dict,
                'processing_summary': {
                    'total_rows': len([item for bc in budget_classes.values() for item in bc.line_items]),
                    'processed_classes': list(budget_classes.keys()),
                    'validation_issues': sum(1 for bc in budget_classes.values() 
                                          for item in bc.line_items 
                                          if item.get('validation_status') == 'warning')
                }
            }
        }

    except Exception as e:
        logger.error(f"Error processing budget: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'error': str(e)
            }
        } 