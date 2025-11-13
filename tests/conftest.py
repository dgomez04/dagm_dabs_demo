import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Fixture to provide a Spark session for tests.
    When running on Databricks, this uses the existing Spark session.
    """
    try:
        # Try to get the existing Databricks Spark session
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        yield spark
    except Exception:
        # Fallback for local testing (though this won't have access to Databricks tables)
        spark = SparkSession.builder \
            .appName("pytest") \
            .master("local[1]") \
            .getOrCreate()
        yield spark
        spark.stop()

