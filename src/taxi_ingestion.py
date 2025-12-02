# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## NYC Taxi Data Pipeline - Medallion Architecture
# MAGIC This notebook implements a Bronze-Silver-Gold data pipeline using NYC taxi trip data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Parameters
# MAGIC Configure catalog and schema for the pipeline

# COMMAND ----------

# Set up job parameters with defaults
dbutils.widgets.text("catalog", "testing", "Catalog")
dbutils.widgets.text("schema", "dagm", "Schema")

# Retrieve parameter values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Pipeline Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## User-Defined Functions (UDFs)
# MAGIC Define custom UDF for trip distance categorization

# COMMAND ----------

# UDF: Categorize trip distance
@F.udf(StringType())
def categorize_trip_distance(distance):
    """
    Categorizes taxi trip distance into human-readable labels.
    
    Args:
        distance (float): Trip distance in miles
        
    Returns:
        str: Distance category - "Short", "Medium", "Long", or "Unknown"
    """
    if distance is None or distance < 0:
        return "Unknown"
    
    if distance < 2.0:
        return "Short"
    elif distance <= 10.0:
        return "Medium"
    else:
        return "Long"

# Register UDF for SQL usage
spark.udf.register("categorize_trip_distance", categorize_trip_distance)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC Ingest raw data from samples catalog

# COMMAND ----------

# Read from samples catalog
df_bronze = spark.read.table("samples.nyctaxi.trips")

# Save to bronze table
bronze_table = f"{catalog}.{schema}.bronze_taxi_trips"
df_bronze.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"Bronze layer created: {bronze_table}")
print(f"Record count: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC Clean and normalize the data

# COMMAND ----------

# Read from bronze
df_silver = spark.read.table(bronze_table)

# Apply data quality filters and transformations
df_silver = (
    df_silver
    # Filter out null values
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .filter(F.col("tpep_dropoff_datetime").isNotNull())
    .filter(F.col("trip_distance").isNotNull())
    .filter(F.col("fare_amount").isNotNull())
    # Filter out invalid values
    .filter(F.col("trip_distance") > 0)  # Remove zero distances
    .filter(F.col("fare_amount") > 0)    # Remove negative or zero fares
    .filter(F.col("trip_distance") < 1000)  # Remove unrealistic distances
    .filter(F.col("fare_amount") < 10000)   # Remove unrealistic fares
    # Add calculated columns
    .withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    )
    # Apply UDF for distance categorization
    .withColumn("distance_category", categorize_trip_distance(F.col("trip_distance")))
)

# Filter out trips with negative duration
df_silver = df_silver.filter(F.col("trip_duration_minutes") > 0)

# Save to silver table
silver_table = f"{catalog}.{schema}.silver_taxi_trips"
df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"Silver layer created: {silver_table}")
print(f"Record count: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Analytics Tables
# MAGIC Create aggregated tables for business analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1: Total Trips by Hour of Day

# COMMAND ----------

# Read from silver
df_gold_source = spark.read.table(silver_table)

# Calculate total trips by hour of day
gold_trips_by_hour = (
    df_gold_source
    .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
    .groupBy("hour_of_day")
    .agg(F.count("*").alias("total_trips"))
    .orderBy("hour_of_day")
)

# Save to gold table
gold_trips_table = f"{catalog}.{schema}.gold_trips_by_hour"
gold_trips_by_hour.write.format("delta").mode("overwrite").saveAsTable(gold_trips_table)

print(f"Gold table created: {gold_trips_table}")
gold_trips_by_hour.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: Average Fare by Hour of Day

# COMMAND ----------

# Calculate average fare by hour of day
gold_avg_fare_by_hour = (
    df_gold_source
    .withColumn("hour_of_day", F.hour("tpep_pickup_datetime"))
    .groupBy("hour_of_day")
    .agg(F.avg("fare_amount").alias("average_fare"))
    .orderBy("hour_of_day")
)

# Save to gold table
gold_fare_table = f"{catalog}.{schema}.gold_avg_fare_by_hour"
gold_avg_fare_by_hour.write.format("delta").mode("overwrite").saveAsTable(gold_fare_table)

print(f"Gold table created: {gold_fare_table}")
gold_avg_fare_by_hour.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample SQL Query Using Registered UDF
# MAGIC Demonstrate using the UDF directly in SQL

# COMMAND ----------

spark.sql(f"""
    SELECT 
        pickup_zip,
        dropoff_zip,
        trip_distance,
        categorize_trip_distance(trip_distance) as distance_category,
        fare_amount,
        trip_duration_minutes
    FROM {silver_table}
    WHERE trip_distance IS NOT NULL
    ORDER BY trip_distance DESC
    LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

print("=" * 60)
print("NYC TAXI DATA PIPELINE - SUMMARY")
print("=" * 60)
print(f"\nBronze Table: {bronze_table}")
print(f"  Records: {spark.read.table(bronze_table).count():,}")
print(f"\nSilver Table: {silver_table}")
print(f"  Records: {spark.read.table(silver_table).count():,}")
print(f"\nGold Tables:")
print(f"  1. {gold_trips_table}")
print(f"     Records: {spark.read.table(gold_trips_table).count():,}")
print(f"  2. {gold_fare_table}")
print(f"     Records: {spark.read.table(gold_fare_table).count():,}")
print("\n" + "=" * 60)
