import os
import sys
# Add the src directory (absolute path) to sys.path so tests can import from budget_sync
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, src_path)
