import pytest
from databricks.sdk import WorkspaceClient

silver_table = "testing.dagm.silver_yahoo_finance"

def test_silver_schema(spark):
    df = spark.table(silver_table)

    expected_columns = {
        "NAME", "COMPANY_ID", "ENTITY_TYPE", "SUMMARY",
        "STOCK_TICKER", "CURRENCY", "EARNINGS_DATE", "EXCHANGE",
        "CLOSING_PRICE", "PREVIOUS_CLOSE", "OPEN", "BID", "ASK",
        "DAY_LOW", "DAY_HIGH", "WEEK_LOW", "WEEK_HIGH",
        "VOLUME", "pct_change", "daily_price_change"
    }

    assert expected_columns.issubset(df.columns)


top_movers_table = "testing.dagm.yahoo_top_movers"

def test_top_movers_unique(spark):
    df = spark.table(top_movers_table)

    # Count occurrences of each exchange
    grouped = df.groupBy("EXCHANGE").count().collect()

    for row in grouped:
        assert row["count"] == 1, f"Exchange {row['EXCHANGE']} has multiple top movers"
