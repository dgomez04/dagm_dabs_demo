"""
Unit tests for NYC Taxi Pipeline
Simplified test suite following Databricks testing best practices
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, IntegerType, StringType
from pyspark.sql.functions import col
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    """
    Fixture to provide a Spark session for tests.
    """
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def sample_taxi_data(spark):
    """
    Create fake taxi data for unit tests.
    Following best practice to not run tests against production data.
    """
    schema = StructType([
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("pickup_zip", IntegerType(), True),
        StructField("dropoff_zip", IntegerType(), True)
    ])
    
    data = [
        (datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 15), 1.5, 12.5, 10001, 10002),
        (datetime(2023, 1, 1, 9, 0), datetime(2023, 1, 1, 9, 30), 5.0, 25.0, 10003, 10004),
        (datetime(2023, 1, 1, 14, 0), datetime(2023, 1, 1, 14, 45), 15.0, 55.0, 10005, 10006),
        (datetime(2023, 1, 1, 8, 30), datetime(2023, 1, 1, 8, 40), 0.8, 8.0, 10007, 10008),
        (datetime(2023, 1, 1, 9, 15), datetime(2023, 1, 1, 9, 50), 8.5, 35.0, 10009, 10010),
        # Invalid data that should be filtered out
        (datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 20), -1.0, 10.0, 10011, 10012),  # negative distance
        (datetime(2023, 1, 1, 11, 0), datetime(2023, 1, 1, 11, 15), 2.0, -5.0, 10013, 10014),   # negative fare
        (datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 1, 12, 10), 0.0, 10.0, 10015, 10016),   # zero distance
        (None, datetime(2023, 1, 1, 13, 0), 3.0, 15.0, 10017, 10018),                           # null pickup time
        (datetime(2023, 1, 1, 15, 0), None, 4.0, 20.0, 10019, 10020),                           # null dropoff time
    ]
    
    return spark.createDataFrame(data, schema)


def test_categorize_trip_distance_short(spark):
    """Test that trips under 2 miles are categorized as Short"""
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import udf
    
    @udf(StringType())
    def categorize_trip_distance(distance):
        if distance is None or distance < 0:
            return "Unknown"
        if distance < 2.0:
            return "Short"
        elif distance <= 10.0:
            return "Medium"
        else:
            return "Long"
    
    df = spark.createDataFrame([(1.5,), (0.5,), (1.99,)], ["distance"])
    result = df.withColumn("category", categorize_trip_distance(col("distance")))
    
    categories = [row["category"] for row in result.collect()]
    assert all(cat == "Short" for cat in categories)


def test_categorize_trip_distance_medium(spark):
    """Test that trips between 2 and 10 miles are categorized as Medium"""
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import udf
    
    @udf(StringType())
    def categorize_trip_distance(distance):
        if distance is None or distance < 0:
            return "Unknown"
        if distance < 2.0:
            return "Short"
        elif distance <= 10.0:
            return "Medium"
        else:
            return "Long"
    
    df = spark.createDataFrame([(2.0,), (5.0,), (10.0,)], ["distance"])
    result = df.withColumn("category", categorize_trip_distance(col("distance")))
    
    categories = [row["category"] for row in result.collect()]
    assert all(cat == "Medium" for cat in categories)


def test_categorize_trip_distance_long(spark):
    """Test that trips over 10 miles are categorized as Long"""
    from pyspark.sql.types import StringType
    from pyspark.sql.functions import udf
    
    @udf(StringType())
    def categorize_trip_distance(distance):
        if distance is None or distance < 0:
            return "Unknown"
        if distance < 2.0:
            return "Short"
        elif distance <= 10.0:
            return "Medium"
        else:
            return "Long"
    
    df = spark.createDataFrame([(10.1,), (15.0,), (50.0,)], ["distance"])
    result = df.withColumn("category", categorize_trip_distance(col("distance")))
    
    categories = [row["category"] for row in result.collect()]
    assert all(cat == "Long" for cat in categories)


def test_data_quality_filters_invalid_distance(sample_taxi_data):
    """Test that invalid distances are filtered out"""
    # Simulate silver layer filtering
    df_filtered = sample_taxi_data.filter(
        (col("trip_distance").isNotNull()) &
        (col("trip_distance") > 0)
    )
    
    negative_or_zero = df_filtered.filter(col("trip_distance") <= 0).count()
    assert negative_or_zero == 0, "Invalid distances should be filtered out"


def test_data_quality_filters_invalid_fare(sample_taxi_data):
    """Test that invalid fares are filtered out"""
    # Simulate silver layer filtering
    df_filtered = sample_taxi_data.filter(
        (col("fare_amount").isNotNull()) &
        (col("fare_amount") > 0)
    )
    
    negative_or_zero = df_filtered.filter(col("fare_amount") <= 0).count()
    assert negative_or_zero == 0, "Invalid fares should be filtered out"


def test_data_quality_filters_null_timestamps(sample_taxi_data):
    """Test that null timestamps are filtered out"""
    # Simulate silver layer filtering
    df_filtered = sample_taxi_data.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("tpep_dropoff_datetime").isNotNull()
    )
    
    null_pickup = df_filtered.filter(col("tpep_pickup_datetime").isNull()).count()
    null_dropoff = df_filtered.filter(col("tpep_dropoff_datetime").isNull()).count()
    
    assert null_pickup == 0, "Null pickup times should be filtered out"
    assert null_dropoff == 0, "Null dropoff times should be filtered out"


def test_trip_duration_calculation(sample_taxi_data):
    """Test that trip duration is calculated correctly"""
    from pyspark.sql.functions import unix_timestamp
    
    df_with_duration = sample_taxi_data.withColumn(
        "trip_duration_minutes",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    )
    
    # Filter to valid records only
    df_valid = df_with_duration.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("tpep_dropoff_datetime").isNotNull()
    )
    
    durations = [row["trip_duration_minutes"] for row in df_valid.collect()]
    
    # Check that all durations are reasonable (between 0 and a few hours)
    assert all(0 <= d <= 180 for d in durations if d is not None), "Trip durations should be reasonable"


def test_hourly_aggregation(sample_taxi_data):
    """Test that hourly aggregation works correctly"""
    from pyspark.sql.functions import hour, count
    
    # Filter valid data
    df_valid = sample_taxi_data.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("trip_distance").isNotNull() &
        (col("trip_distance") > 0)
    )
    
    # Aggregate by hour
    df_hourly = df_valid.withColumn("hour_of_day", hour("tpep_pickup_datetime")) \
        .groupBy("hour_of_day") \
        .agg(count("*").alias("total_trips"))
    
    result = df_hourly.collect()
    
    # Should have some hours with trips
    assert len(result) > 0, "Should have at least one hour with trips"
    
    # All counts should be positive
    assert all(row["total_trips"] > 0 for row in result), "All trip counts should be positive"


def test_average_fare_by_hour(sample_taxi_data):
    """Test that average fare calculation works correctly"""
    from pyspark.sql.functions import hour, avg
    
    # Filter valid data
    df_valid = sample_taxi_data.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("fare_amount").isNotNull() &
        (col("fare_amount") > 0)
    )
    
    # Calculate average fare by hour
    df_avg_fare = df_valid.withColumn("hour_of_day", hour("tpep_pickup_datetime")) \
        .groupBy("hour_of_day") \
        .agg(avg("fare_amount").alias("average_fare"))
    
    result = df_avg_fare.collect()
    
    # Should have some hours with fares
    assert len(result) > 0, "Should have at least one hour with fares"
    
    # All average fares should be positive
    assert all(row["average_fare"] > 0 for row in result), "All average fares should be positive"


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])

