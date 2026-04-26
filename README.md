# AWS-Native Retail Brokerage Data Engineering Pipeline

This repository contains an AWS-native batch data engineering pipeline for a retail stock-broking platform similar to Groww or Zerodha. It covers source data generation, batch ingestion, transformation, data quality validation, orchestration, warehouse loading, infrastructure-as-code, CI/CD, and local test coverage in one end-to-end implementation.

## What this project solves

Retail brokerage platforms generate multiple categories of operational data every day:

- customer and account master data
- tradeable instrument master data
- order lifecycle events
- trade executions
- fund movements
- settlements
- daily holdings snapshots
- market close prices

The objective of this project is to ingest those datasets, validate them, transform them, and load them into an analytics-ready warehouse on **Amazon Redshift Serverless**.

## Services used

- **Amazon S3** for raw and curated storage
- **AWS Glue** for data processing jobs
- **AWS Step Functions** for orchestration and dependency management
- **Amazon EventBridge Scheduler** for a once-daily trigger
- **Amazon SNS** for pipeline success/failure alerts
- **Amazon Redshift Serverless** as the target warehouse
- **AWS CloudFormation** for infrastructure-as-code
- **AWS CodeBuild** for deployment, unit tests, and code coverage

## Project highlights

- Model a brokerage domain using a proper snowflake schema
- Implement **SCD Type 2** dimensions for customer and account history
- Orchestrate multiple Glue jobs with waits and downstream dependencies
- Publish data quality outputs separately from transformation outputs
- Use Redshift Serverless instead of a provisioned cluster
- Keep Python code simple and understandable without unnecessary OOP
- Run local unit tests and coverage before pushing changes

## Business use case

The brokerage domain modeled in this project covers customer onboarding, account management, equity trading, trade settlement, and portfolio analytics. Analysts and business teams can use the warehouse to answer questions such as:

- How much gross trading volume and brokerage revenue was generated each day?
- Which instruments and sectors are most actively traded?
- How many current customers are in each risk band?
- Which accounts changed segment historically?
- What is the market value of customer holdings by sector and day?

## Project architecture

The architecture is separated into clear layers:

1. **Synthetic source generator**
   Generates three days of realistic brokerage source data with embedded change scenarios and controlled DQ failures.

2. **Raw layer**
   Daily source-style CSV partitions are stored under the raw bucket in S3.

3. **Glue transformation layer**
   Separate Glue jobs process:
   - master snapshots
   - transactional data
   - DQ publication
   - Redshift load execution into Redshift Serverless

4. **Warehouse layer**
   Redshift Serverless stores the final snowflake schema and supports analytics queries.

5. **Orchestration layer**
   EventBridge Scheduler triggers Step Functions once per day. Step Functions manages order, waits, dependencies, and SNS notifications.

## AWS execution flow

The AWS deployment path is split into two distinct parts:

1. **Infrastructure deployment**
   CloudFormation creates the VPC, subnets, security groups, S3 buckets, IAM roles, Glue jobs, Step Functions state machine, EventBridge schedule, SNS topic, and **Redshift Serverless** namespace and workgroup.

2. **Daily pipeline execution**
   EventBridge Scheduler starts the Step Functions state machine once per day. The state machine then runs the Glue jobs in the required order and publishes final status through SNS.

### What happens in each AWS step

1. **CodeBuild starts on push to `main`**
   CodeBuild installs Python dependencies, deploys the CloudFormation stack, uploads repository assets to S3, runs unit tests, and generates code coverage reports.

2. **CloudFormation creates or updates infrastructure**
   The template in `infra/cloudformation/template.yaml` provisions all required AWS resources, including:
   - `AWS::RedshiftServerless::Namespace`
   - `AWS::RedshiftServerless::Workgroup`
   - `AWS::Glue::Job`
   - `AWS::StepFunctions::StateMachine`
   - `AWS::Scheduler::Schedule`
   - `AWS::SNS::Topic`

3. **CodeBuild uploads executable assets**
   Glue job scripts, Step Functions definition, Redshift SQL assets, and generated source data are uploaded to S3 so the runtime services can access them.

4. **EventBridge Scheduler triggers the state machine**
   Scheduler sends a fixed input payload to Step Functions once per day. That payload contains the names of Glue jobs, S3 roots, SNS topic ARN, and the Redshift IAM role ARN.

5. **Step Functions resolves the runtime business date**
   The workflow first checks whether `business_date` was provided in the execution input. If it was provided, that exact value is used. Otherwise the state machine derives `business_date` from `$$.Execution.StartTime`. The EventBridge schedule itself is configured in `Asia/Kolkata`, and the default run time is `23:30 IST`, so the derived execution date and the intended IST business date stay aligned for the scheduled run.

6. **Master-data Glue job runs**
   `job_master_data.py` reads daily customer and account snapshots from the raw layer, runs DQ checks, writes curated master outputs, and stores DQ summary JSON.

7. **Step Functions waits for delayed transaction arrival**
   The workflow pauses for `45` seconds before launching the transaction job. This models real-world dependency handling where downstream files may land later than master/reference files.

8. **Transaction-data Glue job runs**
   `job_transaction_data.py` reads orders, trades, fund movements, settlements, market prices, and holdings snapshots. It performs DQ checks, derives fact-ready trade measures, writes curated outputs, and stores DQ summaries.

9. **DQ publish Glue job runs**
   `job_publish_dq.py` aggregates master and transaction DQ summaries into a single pipeline-level audit output.

10. **Redshift load job runs**
    `job_redshift_load.py` stages curated and reference datasets, loads the snowflake dimensions in dependency order, applies SCD2 logic, reloads the daily fact slice, writes the SQL bundle to the curated layer for auditability, submits the statements to Redshift Serverless through the Redshift Data API, and waits for completion.

11. **SNS publishes final pipeline status**
    If all upstream states succeed, the state machine publishes a success notification. Any state failure is caught and routed to the failure notification path.

## Redshift Serverless usage

This repository is built around **Amazon Redshift Serverless**, not provisioned Redshift.

What is already implemented:

- CloudFormation creates `AWS::RedshiftServerless::Namespace`
- CloudFormation creates `AWS::RedshiftServerless::Workgroup`
- warehouse DDL is written for Redshift
- IAM permissions include Redshift Data API and Redshift Serverless credentials access
- the load job executes against Redshift Serverless using `batch_execute_statement`

What is not yet executed in this local workspace:

- the CloudFormation stack has **not** been deployed from this environment
- an actual Redshift Serverless workgroup has **not** been provisioned in your AWS account by me yet
- the Data API execution path can only be exercised after deployment in your AWS account

So the repo is correctly designed for your Redshift Serverless requirement, but the actual AWS resource creation will happen only when you deploy the CloudFormation stack in your AWS account.

### How Glue connects to Redshift Serverless

The Redshift load Glue job does not use JDBC. It uses the **Amazon Redshift Data API**.

Runtime flow:

1. The Glue job receives `workgroup_name` and `database_name`.
2. The Glue job calls `redshift-data:BatchExecuteStatement`.
3. Redshift Serverless derives temporary database credentials from the Glue IAM role by using `redshift-serverless:GetCredentials`.
4. The submitted SQL runs inside the target Redshift Serverless workgroup and database.
5. The SQL uses `COPY ... FROM 's3://...' IAM_ROLE '...'` to load curated S3 data into warehouse tables.
6. The Glue job polls `DescribeStatement` until Redshift finishes or fails.

## CloudFormation rerun behavior

This project uses `aws cloudformation deploy`, which is idempotent for unchanged infrastructure definitions.

What happens on the next deployment:

- if the stack does not exist, CloudFormation creates it
- if the stack already exists and there are no effective changes, CloudFormation performs a no-op update
- if only mutable properties change, CloudFormation updates the existing resources in place
- if an immutable property changes, CloudFormation may replace that specific resource

In other words, running the same deployment again does **not** recreate all resources by default. Re-creation happens only for resources whose configuration changes require replacement according to CloudFormation behavior.

## Snowflake schema design

The warehouse is **not** a flat star schema. It includes snowflaked dimension branches to reflect a more realistic enterprise model.

### Dimensions

- `dim_country`
- `dim_state`
- `dim_city`
- `dim_sector`
- `dim_company`
- `dim_exchange`
- `dim_instrument`
- `dim_advisor_scd2`
- `dim_customer_scd2`
- `dim_account_scd2`
- `dim_date`

### Where each dimension is loaded

All of the dimensions listed above are loaded inside [src/glue/jobs/job_redshift_load.py](/Users/shashankmishra/Desktop/AWS_Services_Mastery_Bootcamp/Project%20Session-2/retail_brokrage_tmp/src/glue/jobs/job_redshift_load.py).

- `dim_date` is inserted directly from the current `business_date`.
- `dim_country`, `dim_state`, and `dim_city` are loaded from `stg_customer_snapshot`, which is copied from the curated `customers_snapshot` output.
- `dim_sector`, `dim_company`, `dim_exchange`, and `dim_instrument` are loaded from `stg_instrument_reference`, which is copied from `s3://<raw-bucket>/reference/instruments.csv`.
- `dim_advisor_scd2` is maintained from `stg_advisor_reference`, which is copied from `s3://<raw-bucket>/reference/advisors.csv`.
- `dim_customer_scd2` is maintained from `stg_customer_snapshot`.
- `dim_account_scd2` is maintained from `stg_account_snapshot`, which is copied from the curated `accounts_snapshot` output.

The load order in that script is:

1. Create or clean staging tables
2. Copy curated snapshots and reference data into staging
3. Load non-SCD snowflake dimensions
4. Apply SCD2 close-and-insert logic
5. Reload the current-day fact tables

### Facts

- `fact_orders`
- `fact_trades`
- `fact_fund_movements`
- `fact_settlements`
- `fact_daily_holdings`

### Snowflake branches

- customer -> city -> state -> country
- instrument -> company -> sector
- instrument -> exchange
- account -> advisor
- account -> customer

## SCD Type 2 implementation

SCD2 is modeled for:

- `dim_customer_scd2`
- `dim_account_scd2`
- optionally extendable to `dim_advisor_scd2`

Each SCD2 row uses:

- `record_start_date`
- `record_end_date`
- `is_current`

This supports historical correctness for dimensions whose attributes change over time, such as:

- customer risk band
- customer location
- account segment
- account status
- advisor assignment

## Data quality strategy

Each Glue job performs dataset-specific checks before curated output is published.

### Example DQ rules

- mandatory field checks
- duplicate business key checks
- allowed value checks
- positive numeric value checks

### DQ output

DQ results are written as JSON summaries under the curated layer and are intended to be publishable to:

- S3 audit paths
- CloudWatch logs
- Redshift audit tables

## Step Functions orchestration flow

1. Run master-data Glue job
2. Wait for delayed transaction feed window
3. Run transaction-data Glue job
4. Run DQ publish job
5. Run Redshift load job
6. Publish SNS success or failure notification

This mirrors how production workflows often need timed dependencies rather than just a simple linear chain.

## EventBridge Scheduler payload

The EventBridge Scheduler target payload configured in CloudFormation is:

```json
{
  "sns_topic_arn": "<SNS topic ARN created by CloudFormation>",
  "master_job_name": "<Glue master job name>",
  "transaction_job_name": "<Glue transaction job name>",
  "dq_job_name": "<Glue DQ publish job name>",
  "redshift_load_job_name": "<Glue Redshift load job name>",
  "raw_root": "s3://<raw-bucket-name>/raw",
  "processed_root": "s3://<curated-bucket-name>",
  "curated_bucket_name": "<curated-bucket-name>",
  "redshift_iam_role_arn": "<Redshift COPY role ARN>"
}
```

### Runtime interpretation of that payload

- `sns_topic_arn`: used by the success and failure notification states
- `master_job_name`: passed to the first Glue task
- `transaction_job_name`: passed to the second Glue task
- `dq_job_name`: passed to the DQ publish Glue task
- `redshift_load_job_name`: passed to the Redshift load Glue task
- `raw_root`: S3 source path for daily raw datasets
- `processed_root`: S3 target path for curated outputs and DQ summaries
- `curated_bucket_name`: used by the Redshift load step to build COPY paths
- `redshift_iam_role_arn`: passed to the Redshift load step as the Redshift S3 access role used by `COPY`

### Important note about `business_date`

`business_date` is optional in the execution input.

- For the daily EventBridge-triggered run, the payload does not provide `business_date`, so the state machine derives it from `$$.Execution.StartTime`.
- For a manual Step Functions execution, you can and should pass `business_date` explicitly if you want to run a specific partition such as `2026-04-19`.
- This is especially useful for manual executions before `05:30 IST`, because Step Functions timestamps are UTC-based.

Example manual execution payload:

```json
{
  "business_date": "2026-04-19",
  "sns_topic_arn": "<SNS topic ARN created by CloudFormation>",
  "master_job_name": "<Glue master job name>",
  "transaction_job_name": "<Glue transaction job name>",
  "dq_job_name": "<Glue DQ publish job name>",
  "redshift_load_job_name": "<Glue Redshift load job name>",
  "raw_root": "s3://<raw-bucket-name>/raw",
  "processed_root": "s3://<curated-bucket-name>",
  "curated_bucket_name": "<curated-bucket-name>",
  "redshift_iam_role_arn": "<Redshift COPY role ARN>",
  "redshift_workgroup_name": "<Redshift Serverless workgroup name>",
  "redshift_database_name": "brokerage"
}
```

## Repository structure

```text
.
├── buildspec.yml
├── docs/
│   └── architecture.md
├── generated_data/
│   ├── processed/
│   └── raw/
├── infra/
│   └── cloudformation/
│       └── template.yaml
├── src/
│   ├── data_generator/
│   │   └── generate_brokerage_data.py
│   ├── glue/
│   │   └── jobs/
│   │       ├── job_master_data.py
│   │       ├── job_publish_dq.py
│   │       ├── job_redshift_load.py
│   │       └── job_transaction_data.py
│   ├── redshift/
│   │   ├── ddl/
│   │   │   └── 001_create_brokerage_warehouse.sql
│   │   └── dml/
│   │       └── analytics_queries.sql
│   └── stepfunctions/
│       └── state_machine.asl.json
├── tests/
│   └── unit/
└── requirements.txt
```

## Synthetic data generation

The project includes a generator that creates three days of brokerage source data with realistic relationships and controlled change events.

### Generated datasets

- `customers_snapshot.csv`
- `accounts_snapshot.csv`
- `orders.csv`
- `trades.csv`
- `fund_movements.csv`
- `settlements.csv`
- `market_prices.csv`
- `holdings_snapshot.csv`
- `reference/advisors.csv`
- `reference/instruments.csv`

### Local command

```bash
python3 -m src.data_generator.generate_brokerage_data \
  --output-root generated_data \
  --start-date 2026-04-19 \
  --days 3 \
  --customers 500 \
  --accounts 800 \
  --instruments 120 \
  --orders-per-day 3500
```

## Local development flow

The local development path is intended for source-data generation, repository walkthroughs, unit tests, and coverage validation.

### 1. Generate raw data

```bash
python3 -m src.data_generator.generate_brokerage_data
```

What happens in this step:

- generates 3 days of source-style brokerage data starting from the current date in `Asia/Kolkata`
- creates daily partitions under `generated_data/raw/business_date=YYYY-MM-DD/`
- writes reference files under `generated_data/reference/`
- injects controlled change scenarios for SCD2 behavior
- injects a controlled invalid order record so the DQ path produces measurable failures

If run on `2026-04-19` in IST, the generated partitions will be:

- `business_date=2026-04-19`
- `business_date=2026-04-20`
- `business_date=2026-04-21`

## AWS Glue execution flow

The Glue ETL scripts below are intended to run inside the AWS Glue runtime, not in a plain local Python shell. Step Functions passes these arguments during scheduled execution.

### 2. Run the master-data Glue job

Example runtime arguments:

```text
--raw_root s3://<raw-bucket>/raw
--processed_root s3://<curated-bucket>
--business_date 2026-04-19
```

What happens in this step:

- reads `customers_snapshot.csv` and `accounts_snapshot.csv`
- uses AWS Glue PySpark with `GlueContext`, `Job`, and `EvaluateDataQuality`
- checks completeness, uniqueness, and allowed values through Glue Data Quality rulesets
- writes curated master outputs under `generated_data/processed/curated/.../master/`
- writes dataset-level DQ result datasets under `generated_data/processed/curated/.../dq/`

### 3. Run the transaction-data Glue job

Example runtime arguments:

```text
--raw_root s3://<raw-bucket>/raw
--processed_root s3://<curated-bucket>
--business_date 2026-04-19
```

What happens in this step:

- reads `orders`, `trades`, `fund_movements`, `settlements`, `market_prices`, and `holdings_snapshot`
- uses AWS Glue PySpark with `GlueContext`, `Job`, and `EvaluateDataQuality`
- validates transactional data quality through Glue Data Quality rulesets
- derives `gross_trade_amount` and `net_trade_amount`
- writes curated transaction outputs under `generated_data/processed/curated/.../transactions/`
- writes transaction DQ result datasets and transaction metrics

### 4. Run the DQ publish Glue job

Example runtime arguments:

```text
--processed_root s3://<curated-bucket>
--business_date 2026-04-19
```

What happens in this step:

- reads curated master and transaction outputs produced by the upstream Glue jobs
- uses AWS Glue PySpark with `GlueContext`, `Job`, and `EvaluateDataQuality`
- runs post-curation Glue DQ rulesets and publishes results to the Glue Data Quality dashboard
- writes the raw Glue DQ result rows to S3 for audit/debugging
- writes a DQ summary dataset to S3
- does not fail the Glue job just because business-rule violations are detected in DQ

### 5. Run the Redshift load Glue job

Example runtime arguments:

```text
--raw_root s3://<raw-bucket>/raw
--processed_root s3://<curated-bucket>
--business_date 2026-04-19
--iam_role_arn arn:aws:iam::<account-id>:role/<redshift-copy-role>
--data_bucket <curated-bucket>
--workgroup_name <redshift-serverless-workgroup-name>
--database_name brokerage
--secret_arn <redshift-admin-secret-arn>
```

What happens in this step:

- copies curated customer/account snapshots and raw reference files into Redshift staging tables
- loads `dim_date`, geography dimensions, instrument snowflake dimensions, and SCD2 dimensions in sequence
- clears the current business-date slice from each fact table and reloads it from curated transaction outputs
- builds Redshift SQL for the full warehouse load sequence
- writes the SQL bundle to `generated_data/processed/curated/.../sql/redshift_load.sql`
- submits the statement batch to Redshift Serverless through the Redshift Data API by using the Redshift admin secret created by CloudFormation
- waits for completion and fails the Glue job if Redshift returns an error

## Unit tests and code coverage

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run tests

```bash
pytest tests/unit -v
```

### Run coverage in the terminal

```bash
pytest tests/unit --cov=src --cov-report=term-missing
```

This prints the coverage percentage and the file-by-file missing lines directly in the terminal.

### Generate coverage reports

```bash
pytest tests/unit --cov=src --cov-report=term-missing --cov-report=html --cov-report=xml
```

This generates:

- `coverage.xml` for CI tools such as CodeBuild or Sonar-style reporting
- `htmlcov/index.html` for a browser-style local coverage report

### Easier local commands

```bash
make test
make coverage
```

After running `make coverage`, open `htmlcov/index.html` locally to inspect the full coverage report.

## CloudFormation deployment

The main infrastructure template is:

- `infra/cloudformation/template.yaml`

### Resources created

- VPC and subnets
- security group with Redshift port `5439`
- S3 buckets for artifact, raw, and curated layers
- Glue IAM role
- Step Functions IAM role
- EventBridge Scheduler IAM role
- SNS topic and email subscription
- Glue jobs
- Redshift Serverless namespace and workgroup
- Step Functions state machine
- EventBridge daily schedule

### Example deployment command

```bash
aws cloudformation deploy \
  --template-file infra/cloudformation/template.yaml \
  --stack-name retail-brokerage-pipeline-dev \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName=retail-brokerage-pipeline \
    EnvironmentName=dev \
    ArtifactBucketName=<your-artifact-bucket> \
    RawDataBucketName=<your-raw-bucket> \
    CuratedDataBucketName=<your-curated-bucket> \
    NotificationEmail=<your-email> \
    AllowedIngressCidr=<your-ip-cidr> \
    DailyScheduleExpression="cron(30 23 * * ? *)" \
    RedshiftAdminUsername=adminuser \
    RedshiftDatabaseName=brokerage \
    RedshiftAdminPassword=<your-password>
```

## CodeBuild setup

You mentioned that you will create the CodeBuild project manually and connect it to GitHub. In that setup, this repository already includes the needed `buildspec.yml`.

### Expected environment variables in CodeBuild

- `STACK_NAME`
- `PROJECT_NAME`
- `ENVIRONMENT_NAME`
- `ARTIFACT_BUCKET_NAME`
- `RAW_DATA_BUCKET_NAME`
- `CURATED_DATA_BUCKET_NAME`
- `NOTIFICATION_EMAIL`
- `ALLOWED_INGRESS_CIDR`
- `DAILY_SCHEDULE_EXPRESSION`
- `REDSHIFT_ADMIN_USERNAME`
- `REDSHIFT_DATABASE_NAME`
- `REDSHIFT_ADMIN_PASSWORD`

### Recommended sample values

| Variable | Example value | Purpose |
|---|---|---|
| `STACK_NAME` | `retail-brokerage-pipeline-prod` | CloudFormation stack name |
| `PROJECT_NAME` | `retail-brokerage-pipeline` | Prefix for named AWS resources |
| `ENVIRONMENT_NAME` | `dev` | Environment suffix used in resource naming |
| `ARTIFACT_BUCKET_NAME` | `retail-brokerage-prod-artifacts-12345` | Stores Glue scripts, SQL, and state machine assets |
| `RAW_DATA_BUCKET_NAME` | `retail-brokerage-prod-raw-12345` | Stores raw source-style daily files |
| `CURATED_DATA_BUCKET_NAME` | `retail-brokerage-prod-curated-12345` | Stores curated outputs, DQ files, and SQL artifacts |
| `NOTIFICATION_EMAIL` | `your-email@example.com` | SNS email subscription for pipeline alerts |
| `ALLOWED_INGRESS_CIDR` | `49.37.10.25/32` | Your laptop or office public IP allowed to access Redshift port `5439` |
| `DAILY_SCHEDULE_EXPRESSION` | `cron(30 23 * * ? *)` | Daily schedule interpreted in `Asia/Kolkata`, defaulting to `23:30 IST` |
| `REDSHIFT_ADMIN_USERNAME` | `adminuser` | Redshift Serverless admin username |
| `REDSHIFT_DATABASE_NAME` | `brokerage` | Target Redshift Serverless database name |
| `REDSHIFT_ADMIN_PASSWORD` | `ChangeThisToAStrongPassword123!` | Redshift Serverless admin password |

### Notes for choosing values

- Bucket names must be globally unique across AWS.
- `ALLOWED_INGRESS_CIDR` should ideally be your current public IP with `/32`, not `0.0.0.0/0`.
- EventBridge Scheduler in this template explicitly uses `Asia/Kolkata` timezone.
- `REDSHIFT_ADMIN_PASSWORD` should be stored as a secured environment variable in CodeBuild.

### What CodeBuild will do

1. Install Python dependencies
2. Deploy CloudFormation, including the Redshift Serverless namespace and workgroup
3. Upload project code and generated data to S3
4. Run unit tests
5. Run code coverage

## Important implementation notes

- The project uses **Redshift Serverless** as requested.
- Glue jobs are implemented as AWS Glue PySpark ETL jobs using `GlueContext`, `Job`, and `EvaluateDataQuality`.
- Core business logic is kept in plain functions so unit testing is easy.
- The provided state machine includes an explicit wait step to model delayed transaction feed arrival.
- The generated source data contains a controlled bad order record so the DQ path surfaces measurable validation failures.

## Analytics assets

Example warehouse assets are provided here:

- DDL: `src/redshift/ddl/001_create_brokerage_warehouse.sql`
- example analytics queries: `src/redshift/dml/analytics_queries.sql`

## Recommended next improvements

The following enhancements would take the implementation closer to a hardened production deployment:

1. Convert curated outputs from CSV to parquet and optimize partition strategy.
2. Add post-load validation queries and row-count reconciliation after Redshift execution.
3. Store DQ summaries in a dedicated audit schema inside Redshift.
4. Add schema validation and source-file completeness checks in Step Functions.
5. Introduce partition pruning, late-arrival handling, and replay support.

## Summary

This repository brings together the core building blocks of a modern AWS data engineering platform:

- domain modeling
- synthetic source generation
- DQ enforcement
- batch orchestration
- Redshift Serverless warehousing
- SCD2 history management
- testing
- coverage
- infrastructure-as-code
- CI/CD

The codebase is structured to keep execution flow, infrastructure, and transformation logic easy to follow during implementation reviews and technical discussions.
