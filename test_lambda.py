import json
from src.budget_sync.lambda_handler import lambda_handler

def test_lambda():
    # Read the test event
    with open('events/test_event.json', 'r') as f:
        test_event = json.load(f)
    
    # Invoke the lambda handler
    try:
        response = lambda_handler(test_event, None)
        print("Lambda execution successful!")
        print("Response:", json.dumps(response, indent=2))
    except Exception as e:
        print("Error executing lambda:", str(e))

if __name__ == "__main__":
    test_lambda() 