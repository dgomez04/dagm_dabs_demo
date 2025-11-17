#!/usr/bin/env python
"""
Script to run pytest tests on Databricks cluster.
This script is executed as a spark_python_task in a Databricks job.
"""
import sys
import os
import pytest

if __name__ == "__main__":
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Run pytest on the same directory (tests/)
    # This ensures we find conftest.py and test files correctly
    exit_code = pytest.main([
        script_dir,
        "-v",  # Verbose output
        "--tb=short"  # Shorter traceback format
    ])
    
    # Exit with the pytest exit code (0 for success, non-zero for failures)
    sys.exit(exit_code)

