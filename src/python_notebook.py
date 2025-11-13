# Databricks notebook source
from pyspark.sql import functions as F

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
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("ns_demo_db.dagm.silver_yahoo_finance")

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
