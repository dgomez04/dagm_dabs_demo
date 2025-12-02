# NYC Taxi Data Pipeline - DABs Demo

This project demonstrates a Databricks Asset Bundle (DABs) implementation featuring a medallion architecture (Bronze-Silver-Gold) for NYC taxi trip data analysis.

## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    The deployment will create a job called `[dev yourname] NYC Taxi Pipeline` 
    to your workspace. You can find that job by opening your workspace and 
    clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the production job has a schedule that runs every day at 3 AM PST
   (defined in resources/taxi_pipeline.yml). The schedule is paused when 
   deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html. Or read the "getting started" documentation for
   **Databricks Connect** for instructions on running the included Python code from a different IDE.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.

## Pipeline Architecture

This project implements a medallion architecture for processing NYC taxi trip data:

### Bronze Layer
- **Source**: `samples.nyctaxi.trips` (built-in Databricks dataset)
- **Output**: `{catalog}.{schema}.bronze_taxi_trips`
- **Description**: Raw data ingestion from the samples catalog

### Silver Layer
- **Input**: Bronze table
- **Output**: `{catalog}.{schema}.silver_taxi_trips`
- **Transformations**:
  - Filters out null and invalid values (negative fares, zero distances)
  - Adds calculated column: `trip_duration_minutes`
  - Applies UDF to categorize trip distances (Short, Medium, Long)
  - Data quality checks for realistic values

### Gold Layer
- **Input**: Silver table
- **Outputs**: 
  - `{catalog}.{schema}.gold_trips_by_hour` - Total trips aggregated by hour of day
  - `{catalog}.{schema}.gold_avg_fare_by_hour` - Average fare aggregated by hour of day
- **Description**: Analytics-ready tables for business insights

### Job Parameters
The pipeline uses job-level parameters for flexible configuration:
- `catalog` (default: "testing") - Target catalog for tables
- `schema` (default: "dagm") - Target schema for tables

These can be configured when running the job via the Databricks UI or API.

### User-Defined Functions
- `categorize_trip_distance`: Categorizes trips as Short (<2 miles), Medium (2-10 miles), or Long (>10 miles)

## CI/CD Pipeline

The project uses GitHub Actions for continuous integration and deployment with environment-specific workflows:

### Pull Request Workflow (Dev Environment)
When you create a pull request to `main`:
1. ✅ **Run Unit Tests** - Tests must pass with 80%+ pass rate
2. ✅ **Deploy to Dev** - Deploy bundle to dev environment
3. ✅ **Run Pipeline in Dev** - Execute the taxi pipeline in dev for validation

### Merge to Main Workflow (Prod Environment)
When you merge to `main`:
1. ✅ **Run Unit Tests** - Tests must pass with 80%+ pass rate
2. ✅ **Deploy to Prod** - Deploy bundle to production environment
3. ✅ **Run Pipeline in Prod** - Execute the taxi pipeline in production

This ensures:
- All code is tested before deployment
- Dev environment is used for validation during PR review
- Production deployments only happen from the main branch
- Scheduled execution only runs in production

## Testing

The project includes a comprehensive test suite following Databricks testing best practices:

- **Test Framework**: pytest with PySpark
- **Test Location**: `tests/test_taxi_pipeline.py`
- **Coverage Requirement**: 80% minimum pass rate
- **CI/CD Integration**: Tests run automatically in GitHub Actions before deployment

### Running Tests Locally

```bash
# Install test dependencies
pip install pytest pytest-cov pyspark

# Run tests
pytest tests/ -v

# Run tests with coverage
pytest tests/ -v --cov=src --cov-report=term
```

### Test Coverage

The CI/CD pipeline enforces an 80% test pass rate. If fewer than 80% of tests pass, the pipeline will fail and prevent deployment to production. This ensures code quality and reliability.

##  This project can be adapted to work with any git provider. 

#### Note: The provided pipelines serve as base examples and will need to be customized to meet the specific requirements of your projects. 

## Pipeline examples:

- **Azure DevOps**:
   [Pipeline for Azure DevOps](./pipeline_example_by_provider/azure-pipelines.yml)

- **BitBucket**:
   [Pipeline for BitBucket](./pipeline_example_by_provider/bitbucket-pipelines.yml)

- **Github Actions**:
   [Pipeline Github](./pipeline_example_by_provider/github-actions.yml)

- **Gitlab**:
   [Pipeline for Gitlab](./pipeline_example_by_provider/gitlab-ci.yml)

- **Jenkins**:
   - [Pipeline for Jenkins (Pipeline Version)](./pipeline_example_by_provider/jenkins/Jenkinsfile)
   - [Pipeline for Jenkins (Freestyle Version)](./pipeline_example_by_provider/jenkins/jenkins_deploy.sh)

   **Note**: The Jenkins agent (also known as a node) must be properly configured with the necessary tools and libraries installed to execute the pipeline. This includes, but is not limited to, Databricks CLI, Python, and other dependencies required by your pipeline. [Configure a node in Jenkins](https://www.jenkins.io/blog/2022/12/27/run-jenkins-agent-as-a-service/#create-a-new-node-in-jenkins)