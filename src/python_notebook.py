# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## User-Defined Functions (UDFs)
# MAGIC Define custom UDFs for financial data processing

# COMMAND ----------

# UDF 1: Categorize stock performance based on percentage change
@F.udf(StringType())
def categorize_performance(pct_change):
    """
    Categorizes stock performance into human-readable labels.
    Returns None if pct_change is None.
    """
    if pct_change is None:
        return "Unknown"
    
    if pct_change >= 0.05:
        return "Strong Gain"
    elif pct_change >= 0.02:
        return "Moderate Gain"
    elif pct_change >= -0.02:
        return "Flat"
    elif pct_change >= -0.05:
        return "Moderate Loss"
    else:
        return "Strong Loss"

# Register UDF for SQL usage
spark.udf.register("categorize_performance", categorize_performance)

# COMMAND ----------

# UDF 2: Calculate volatility indicator based on day range
def calculate_volatility(day_low, day_high, closing_price):
    """
    Calculates a simple volatility metric as percentage of daily range.
    Returns None if any input is None or closing_price is zero.
    """
    if day_low is None or day_high is None or closing_price is None or closing_price == 0:
        return None
    
    daily_range = day_high - day_low
    volatility_pct = (daily_range / closing_price) * 100
    return round(volatility_pct, 2)

# Register as UDF with proper return type
volatility_udf = F.udf(calculate_volatility, DoubleType())
spark.udf.register("calculate_volatility", calculate_volatility, DoubleType())

# COMMAND ----------

# UDF 3: Format volume numbers in human-readable format (e.g., 1.5M, 2.3B)
@F.udf(StringType())
def format_volume(volume):
    """
    Formats large volume numbers into readable strings.
    Examples: 1500000 -> "1.5M", 2300000000 -> "2.3B"
    """
    if volume is None or volume == 0:
        return "0"
    
    try:
        volume = float(volume)
        if volume >= 1_000_000_000:
            return f"{volume / 1_000_000_000:.1f}B"
        elif volume >= 1_000_000:
            return f"{volume / 1_000_000:.1f}M"
        elif volume >= 1_000:
            return f"{volume / 1_000:.1f}K"
        else:
            return f"{volume:.0f}"
    except (ValueError, TypeError):
        return "N/A"

spark.udf.register("format_volume", format_volume)

# COMMAND ----------

df_bronze = spark.read.table("yahoo_fb.datasets.yahoo_finance_business")

def to_float(col):
    return F.expr(f"try_cast(regexp_replace({col}, ',', '') as float)")

df_silver = (
    df_bronze
    .withColumn("CLOSING_PRICE", to_float("CLOSING_PRICE"))
    .withColumn("PREVIOUS_CLOSE", to_float("PREVIOUS_CLOSE"))
    .withColumn("OPEN", to_float("OPEN"))
    .withColumn("VOLUME", to_float("VOLUME"))
    .withColumn("BID", F.expr("try_cast(regexp_extract(BID, '([0-9.]+)', 1) as float)"))
    .withColumn("ASK", F.expr("try_cast(regexp_extract(ASK, '([0-9.]+)', 1) as float)"))
    .withColumn("DAY_LOW", F.expr("try_cast(regexp_extract(DAY_RANGE, '([0-9.]+)', 1) as float)"))
    .withColumn("DAY_HIGH", F.expr("try_cast(regexp_extract(DAY_RANGE, '- ([0-9.]+)', 1) as float)"))
    .withColumn("WEEK_LOW", F.expr("try_cast(regexp_extract(WEEK_RANGE, '([0-9.]+)', 1) as float)"))
    .withColumn("WEEK_HIGH", F.expr("try_cast(regexp_extract(WEEK_RANGE, '- ([0-9.]+)', 1) as float)"))
    .withColumn(
        "PEOPLE_ALSO_WATCH",
        F.from_json(
            "PEOPLE_ALSO_WATCH",
            "array<struct<change:float,last_price:float,perc_change:string,symbol:string>>"
        )
    )
    .withColumn("daily_price_change", F.col("CLOSING_PRICE") - F.col("PREVIOUS_CLOSE"))
    .withColumn("pct_change", F.col("daily_price_change") / F.col("PREVIOUS_CLOSE"))
    # Apply UDFs to enrich the data
    .withColumn("performance_category", categorize_performance(F.col("pct_change")))
    .withColumn("volatility_pct", volatility_udf(F.col("DAY_LOW"), F.col("DAY_HIGH"), F.col("CLOSING_PRICE")))
    .withColumn("volume_formatted", format_volume(F.col("VOLUME")))
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("testing.dagm.silver_yahoo_finance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample UDF Usage in SQL
# MAGIC You can also use these UDFs directly in SQL queries since they're registered with spark.udf.register()

# COMMAND ----------

# Example SQL query using the registered UDFs
spark.sql("""
    SELECT 
        STOCK_TICKER,
        CLOSING_PRICE,
        pct_change,
        categorize_performance(pct_change) as performance,
        calculate_volatility(DAY_LOW, DAY_HIGH, CLOSING_PRICE) as volatility,
        format_volume(VOLUME) as readable_volume
    FROM testing.dagm.silver_yahoo_finance
    WHERE pct_change IS NOT NULL
    ORDER BY pct_change DESC
    LIMIT 10
""").display()

# COMMAND ----------

## Top Movers per Exchange
top_movers = (
    df_silver
    .filter(F.col("pct_change").isNotNull())
    .groupBy("EXCHANGE")
    .agg(
        F.max_by("STOCK_TICKER", "pct_change").alias("top_gainer"),
        F.max("pct_change").alias("max_daily_gain")
    )
)

# Exchange Market Summary
market_summary = (
    df_silver.groupBy("EXCHANGE")
        .agg(
            F.avg("pct_change").alias("avg_pct_change"),
            F.sum("VOLUME").alias("total_volume"),
            F.count(F.when(F.col("pct_change") > 0, 1)).alias("winners"),
            F.count(F.when(F.col("pct_change") < 0, 1)).alias("losers")
        )
)

# Entity-Type Insights
entity_type_summary = (
    df_silver.groupBy("ENTITY_TYPE")
        .agg(
            F.count("*").alias("num_assets"),
            F.avg("CLOSING_PRICE").alias("avg_close_price"),
            F.avg("VOLUME").alias("avg_volume")
        )
)


# COMMAND ----------

top_movers.write.format("delta").mode("overwrite").saveAsTable(f"testing.dagm.yahoo_top_movers")

market_summary.write.format("delta").mode("overwrite").saveAsTable(f"testing.dagm.yahoo_market_summary")

entity_type_summary.write.format("delta").mode("overwrite").saveAsTable(f"testing.dagm.yahoo_entity_type_summary")
