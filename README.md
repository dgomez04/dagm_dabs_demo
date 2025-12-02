# NYC Taxi Data Pipeline - Databricks Asset Bundle Demo

A demonstration of Databricks Asset Bundles (DABs) implementing a medallion architecture (Bronze-Silver-Gold) for NYC taxi trip data analysis.

## Quick Start

### Prerequisites
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
- Configured Databricks authentication

### Deploy

```bash
# Authenticate
databricks configure

# Deploy to dev (default target)
databricks bundle deploy

# Deploy to production
databricks bundle deploy --target prod

# Run the pipeline
databricks bundle run taxi_pipeline
```

## Project Structure

```
├── src/
│   └── taxi_ingestion.py       # Main data pipeline notebook
├── tests/
│   └── test_taxi_pipeline.py   # Unit tests
├── resources/
│   └── taxi_pipeline.yml        # Job configuration
└── databricks.yml               # Bundle configuration
```

## Pipeline Architecture

### Medallion Layers

**Bronze Layer**
- Source: `samples.nyctaxi.trips`
- Output: `{catalog}.{schema}.bronze_taxi_trips`
- Description: Raw data ingestion

**Silver Layer**
- Output: `{catalog}.{schema}.silver_taxi_trips`
- Transformations:
  - Data quality filters (nulls, negatives, invalid values)
  - Calculated field: `trip_duration_minutes`
  - Distance categorization UDF (Short/Medium/Long)

**Gold Layer**
- `{catalog}.{schema}.gold_trips_by_hour` - Trip counts by hour
- `{catalog}.{schema}.gold_avg_fare_by_hour` - Average fares by hour

### Parameters

Configure via job parameters or `resources/taxi_pipeline.yml`:
- `catalog` - Target catalog (default: `adb_dagm`)
- `schema` - Target schema (default: `default`)

## CI/CD Pipeline

The GitHub Actions workflow provides environment-specific deployments:

**Pull Requests**
1. Run unit tests (80% pass rate required)
2. Deploy to dev environment
3. Execute pipeline in dev

**Merges to Main**
1. Deploy to prod environment
2. Execute pipeline in prod
3. Enable scheduled runs (daily at 3 AM PST)

Note: Tests only run during pull requests. Production deployments assume tests passed during the PR review process.

## Testing

Run tests locally:

```bash
pip install pytest pytest-cov pyspark
pytest tests/ -v
pytest tests/ -v --cov=src --cov-report=term
```

The test suite validates:
- UDF functionality
- Data quality filters
- Aggregation logic
- Pipeline transformations

## Environment Configuration

**Development**
- Mode: `development`
- Schedule: Disabled
- Manual execution only

**Production**
- Mode: `production`
- Schedule: Daily at 3 AM PST
- Automated deployment from main branch

## Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)
- [Testing in Databricks](https://docs.databricks.com/notebooks/testing.html)
