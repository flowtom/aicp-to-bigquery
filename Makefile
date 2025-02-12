## Makefile for Budget Sync Project

.PHONY: test run sam-local

# Run the test suite using pytest

test:
	pytest tests/

# Run the main budget processing script (adjust as needed)

run:
	python src/budget_sync/scripts/process_budget.py

# Start the API locally using AWS SAM

sam-local:
	sam local start-api --port 3000

# Additional targets can be added here as needed 