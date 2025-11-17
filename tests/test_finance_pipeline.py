# Databricks notebook source
# MAGIC %md
# MAGIC # Finance Pipeline Unit Tests
# MAGIC 
# MAGIC This notebook runs unit tests for the Yahoo Finance data pipeline.
# MAGIC It uses pytest with PySpark testing utilities.

# COMMAND ----------

import pytest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pytest Fixture Setup

# COMMAND ----------

@pytest.fixture(scope="session")
def spark_fixture():
    """
    Fixture to provide a Spark session for tests.
    When running on Databricks, this uses the existing Spark session.
    """
    spark = SparkSession.builder.getOrCreate()
    yield spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Cases

# COMMAND ----------

def test_silver_schema(spark_fixture):
    """Test that the silver table has all expected columns"""
    silver_table = "testing.dagm.silver_yahoo_finance"
    df = spark_fixture.table(silver_table)

    expected_columns = {
        "NAME", "COMPANY_ID", "ENTITY_TYPE", "SUMMARY",
        "STOCK_TICKER", "CURRENCY", "EARNINGS_DATE", "EXCHANGE",
        "CLOSING_PRICE", "PREVIOUS_CLOSE", "OPEN", "BID", "ASK",
        "DAY_LOW", "DAY_HIGH", "WEEK_LOW", "WEEK_HIGH",
        "VOLUME", "pct_change", "daily_price_change",
        "performance_category", "volatility_pct", "volume_formatted"
    }

    actual_columns = set(df.columns)
    assert expected_columns.issubset(actual_columns), \
        f"Missing columns: {expected_columns - actual_columns}"

# COMMAND ----------

def test_silver_numeric_types(spark_fixture):
    """Test that numeric columns have correct data types"""
    silver_table = "testing.dagm.silver_yahoo_finance"
    df = spark_fixture.table(silver_table)

    numeric_cols = [
        "CLOSING_PRICE", "PREVIOUS_CLOSE", "OPEN", "BID", "ASK",
        "DAY_LOW", "DAY_HIGH", "WEEK_LOW", "WEEK_HIGH",
        "VOLUME", "pct_change", "daily_price_change", "volatility_pct"
    ]

    for col_name in numeric_cols:
        if col_name in df.columns:
            col_type = dict(df.dtypes)[col_name]
            assert col_type in ["float", "double", "int", "bigint"], \
                f"Column {col_name} has type {col_type}, expected numeric type"

# COMMAND ----------

def test_silver_no_null_keys(spark_fixture):
    """Test that key columns don't have null values"""
    silver_table = "testing.dagm.silver_yahoo_finance"
    df = spark_fixture.table(silver_table)

    # Only check STOCK_TICKER - EXCHANGE can be null for certain asset types
    # (e.g., cryptocurrencies, OTC stocks, private equity)
    key_columns = ["STOCK_TICKER"]
    
    for col_name in key_columns:
        if col_name in df.columns:
            null_count = df.filter(df[col_name].isNull()).count()
            assert null_count == 0, f"Column {col_name} has {null_count} null values"

# COMMAND ----------

def test_top_movers_unique(spark_fixture):
    """Test that each exchange has exactly one top mover"""
    top_movers_table = "testing.dagm.yahoo_top_movers"
    df = spark_fixture.table(top_movers_table)

    # Count occurrences of each exchange
    grouped = df.groupBy("EXCHANGE").count().collect()

    for row in grouped:
        assert row["count"] == 1, \
            f"Exchange {row['EXCHANGE']} has {row['count']} top movers, expected 1"

# COMMAND ----------

def test_market_summary_schema(spark_fixture):
    """Test that market summary table has expected schema"""
    market_summary_table = "testing.dagm.yahoo_market_summary"
    df = spark_fixture.table(market_summary_table)

    expected_columns = {
        "EXCHANGE", "avg_pct_change", "total_volume", "winners", "losers"
    }

    actual_columns = set(df.columns)
    assert expected_columns.issubset(actual_columns), \
        f"Missing columns: {expected_columns - actual_columns}"

# COMMAND ----------

def test_entity_type_summary_schema(spark_fixture):
    """Test that entity type summary table has expected schema"""
    entity_type_table = "testing.dagm.yahoo_entity_type_summary"
    df = spark_fixture.table(entity_type_table)

    expected_columns = {
        "ENTITY_TYPE", "num_assets", "avg_close_price", "avg_volume"
    }

    actual_columns = set(df.columns)
    assert expected_columns.issubset(actual_columns), \
        f"Missing columns: {expected_columns - actual_columns}"

# COMMAND ----------

def test_performance_category_values(spark_fixture):
    """Test that performance_category UDF produces expected values"""
    silver_table = "testing.dagm.silver_yahoo_finance"
    df = spark_fixture.table(silver_table)

    valid_categories = {
        "Strong Gain", "Moderate Gain", "Flat", 
        "Moderate Loss", "Strong Loss", "Unknown"
    }

    if "performance_category" in df.columns:
        distinct_categories = set(
            row["performance_category"] 
            for row in df.select("performance_category").distinct().collect()
            if row["performance_category"] is not None
        )
        
        invalid_categories = distinct_categories - valid_categories
        assert len(invalid_categories) == 0, \
            f"Found invalid performance categories: {invalid_categories}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run All Tests

# COMMAND ----------

import sys
import os
import inspect

if __name__ == "__main__":
    # In Databricks notebooks, we need to manually collect and run tests
    # Get the Spark session for the fixture
    spark_session = SparkSession.builder.getOrCreate()
    
    # Collect all test functions from the current module
    current_module = sys.modules[__name__]
    test_functions = [
        (name, obj) for name, obj in inspect.getmembers(current_module)
        if inspect.isfunction(obj) and name.startswith('test_')
    ]
    
    print(f"=" * 70)
    print(f"Running {len(test_functions)} test functions")
    print(f"=" * 70)
    print()
    
    passed = 0
    failed = 0
    errors = []
    
    for test_name, test_func in test_functions:
        try:
            print(f"Running: {test_name}...", end=" ")
            
            # Check if test function needs spark_fixture parameter
            sig = inspect.signature(test_func)
            if 'spark_fixture' in sig.parameters:
                test_func(spark_session)
            else:
                test_func()
            
            print("✓ PASSED")
            passed += 1
        except AssertionError as e:
            print("✗ FAILED")
            failed += 1
            errors.append((test_name, str(e)))
        except Exception as e:
            print("✗ ERROR")
            failed += 1
            errors.append((test_name, f"Error: {str(e)}"))
    
    print()
    print(f"=" * 70)
    print(f"Test Results: {passed} passed, {failed} failed")
    print(f"=" * 70)
    
    if errors:
        print("\nFailure Details:")
        print("-" * 70)
        for test_name, error_msg in errors:
            print(f"\n{test_name}:")
            print(f"  {error_msg}")
        print()
        raise SystemExit(f"{failed} test(s) failed!")
    else:
        print("\n✓ All tests passed!")
        print()

