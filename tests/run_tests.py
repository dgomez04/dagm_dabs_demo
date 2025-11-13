#!/usr/bin/env python
"""
Script to run pytest tests on Databricks cluster.
This script is executed as a spark_python_task in a Databricks job.
"""
import sys
import pytest

if __name__ == "__main__":
    # Run pytest with the tests directory
    # Exit with the pytest exit code
    exit_code = pytest.main([
        "tests/",
        "-v",  # Verbose output
        "--tb=short"  # Shorter traceback format
    ])
    
    # Exit with the pytest exit code (0 for success, non-zero for failures)
    sys.exit(exit_code)

