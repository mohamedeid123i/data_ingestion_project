# Data Engineering Ingestion Master Roadmap

This roadmap is the fixed learning plan for the project. We will not renumber or redesign the phases during implementation. We will progress through the phases one by one, and each phase must meet its acceptance criteria before moving forward.

## Current Progress

- Phase 0: Environment and project foundation — completed
- Phase 1: dlt core concepts — completed
- Phase 2: Files ingestion — next

## Learning Principles

- Use Python and dlt.
- Start locally with DuckDB as the destination.
- Use public APIs and a locally controlled PostgreSQL database as sources.
- Keep credentials outside source code.
- Discuss design before implementation.
- Implement in small understandable steps.
- Test both successful and failure scenarios.
- Introduce cloud destinations and orchestration only after the local concepts are clear.

## Phase 0 — Environment and Project Foundation

Topics:

- Python virtual environment
- Project structure
- Installing dlt and DuckDB
- Configuration and secrets
- Git basics
- DuckDB as a local destination
- Reading loaded data with SQL

Acceptance criteria:

- The environment can import dlt and DuckDB.
- A local pipeline can create a DuckDB database.
- Credentials are not stored in source code.

## Phase 1 — dlt Core Concepts

Completed topics:

- Resource
- Pipeline
- Destination
- Dataset and table
- `append`
- `replace`
- `merge`
- `primary_key`
- Load verification
- Re-running a pipeline
- Understanding why `merge` requires a key

Acceptance criteria:

- Load a Python list into DuckDB.
- Replace a table safely.
- Merge records using a declared primary key.
- Verify row counts and sample data.
- Explain the difference between append, replace, and merge.

## Phase 2 — Files Ingestion

Topics:

- CSV ingestion
- JSON ingestion
- File discovery
- Schema differences between files
- Missing values
- Duplicate records
- Append and merge strategies
- Invalid files
- Data quality tests

Learning sequence:

1. CSV full load
2. CSV append
3. CSV merge using a business key
4. Duplicate and missing-value handling
5. JSON loading
6. Invalid JSON and missing-file tests

Acceptance criteria:

- Load CSV and JSON into DuckDB.
- Explain the effect of append, replace, and merge.
- Detect duplicate keys and missing values.
- Handle a changed file schema intentionally.
- Test empty, invalid, and missing files.

## Phase 3 — Normalization

Topics:

- Flattening nested dictionaries
- Nested lists
- Parent and child tables
- Generated keys
- Root keys
- Relationships between normalized tables
- Normalizing JSON into relational tables
- `max_table_nesting`

Acceptance criteria:

- Load nested JSON.
- Identify the root table and child tables.
- Query normalized data with SQL.
- Explain how nested records are connected.

## Phase 4 — REST API Ingestion

Topics:

- `RESTClient`
- Public APIs
- Authentication
- Pagination patterns
- Rate limits
- Retries
- HTTP failures
- DummyJSON Products, Users, and Posts
- Multiple resources
- API response normalization

Acceptance criteria:

- Load DummyJSON products into DuckDB.
- Implement `limit` and `skip` pagination.
- Load users and posts as additional resources.
- Handle empty responses and HTTP failures.
- Verify that the selected pagination and authentication behavior matches the API documentation.

## Phase 5 — Schema Management

Topics:

- Schema inference
- Schema evolution
- Adding columns
- Changing data types
- Removing columns
- Renaming columns
- Variant columns
- Data contracts
- `evolve`
- `freeze`
- `discard_rows`
- `discard_columns`
- Alerting on schema changes

Acceptance criteria:

- Demonstrate an added column.
- Demonstrate a changed data type.
- Demonstrate a missing or renamed column.
- Inspect schema changes in the destination.
- Decide which changes should be accepted or rejected.

## Phase 6 — Incremental Loading

Topics:

- The meaning of incremental loading
- dlt state
- Cursor-based extraction
- Date-based extraction
- `dlt.sources.incremental`
- `initial_value`
- `last_value`
- Watermarks
- Loading only new or changed records

Acceptance criteria:

- Run a pipeline more than once without reloading old records unnecessarily.
- Explain where the incremental state is stored.
- Test a new record and an updated record.
- Test a failed and resumed load.

## Phase 7 — Merge and CDC Patterns

Topics:

- `primary_key`
- `merge_key`
- Upsert
- Delete-insert
- CDC
- Composite keys
- Deduplication
- Records removed from the source

Acceptance criteria:

- Update an existing record by key.
- Insert a new record.
- Use a composite key when one column is not unique.
- Explain the difference between merge strategies.

## Phase 8 — PostgreSQL Source to SQL Destination

Topics:

- Local PostgreSQL as a source
- Ecommerce database schema
- `dlt.sources.sql_database`
- SQLAlchemy
- Full load
- Incremental database extraction
- PostgreSQL to DuckDB
- PostgreSQL to PostgreSQL
- SQL source to a warehouse
- Backfilling
- Foreign keys
- Database connection failures

Acceptance criteria:

- Create customers, products, orders, and order-items tables.
- Load PostgreSQL tables into DuckDB.
- Perform a full load.
- Add and update records incrementally.
- Perform a historical backfill.
- Handle a database connection failure clearly.

## Phase 9 — Slowly Changing Dimensions

Topics:

- SCD Type 1
- SCD Type 2
- Change history
- `valid_from`
- `valid_to`
- Current-record flag
- Snapshot loading
- Current state versus historical state

Acceptance criteria:

- Implement a Type 1 update.
- Preserve history with Type 2.
- Query the current version of a dimension.
- Query the historical version at a past point in time.

## Phase 10 — Time-Series Ingestion

Topics:

- Open-Meteo
- Historical weather data
- Date ranges
- Time-series keys
- Composite keys such as location and timestamp
- Reprocessing the latest period
- Late-arriving data
- Date-based incremental extraction
- Merge for time-series data

Acceptance criteria:

- Load weather data for a selected location and date range.
- Define a stable time-series key.
- Reload the latest period safely.
- Avoid duplicate timestamps.
- Explain why this is different from cursor-based event ingestion.

## Phase 11 — Performance Tuning

Topics:

- Batch size
- Pagination size
- Parallelism
- File formats
- Parquet
- Compression
- Chunking
- Destination loading strategies
- Staging
- Reducing API calls
- Reducing memory usage
- Measuring before and after optimization

Acceptance criteria:

- Measure a baseline load.
- Change one performance setting at a time.
- Compare runtime and memory behavior.
- Explain the trade-offs of the change.

## Phase 12 — Data Lakes and Lakehouses

Topics:

- Filesystem destination
- Local data lake
- S3 and object storage
- Parquet
- Partitioning
- Data lake layers
- Iceberg
- Delta Lake
- External tables
- Catalogs
- Athena
- Metadata management

Acceptance criteria:

- Write data as Parquet.
- Partition a dataset intentionally.
- Explain the difference between files, tables, and catalogs.
- Load or register a lakehouse table.

## Phase 13 — Data Warehouses

Topics:

- BigQuery
- Snowflake
- Redshift
- PostgreSQL as a destination
- Warehouse schemas
- Staging tables
- Loading through staging
- MPP concepts
- COPY-style loading
- Warehouse-specific credentials

Acceptance criteria:

- Select a warehouse destination intentionally.
- Configure credentials outside source code.
- Load data through the appropriate strategy.
- Explain when staging improves performance.

## Phase 14 — Observability and Production Quality

Topics:

- `load_info`
- Pipeline traces
- Logs
- Failed loads
- Retries
- Alerts
- Schema-change alerts
- Row-count checks
- Freshness checks
- Data quality checks
- Lineage
- Notifications

Acceptance criteria:

- Detect a failed load.
- Produce useful logs.
- Alert on a schema change.
- Validate row count and freshness.
- Record enough metadata to debug a load.

## Phase 15 — Deployment and Orchestration

Topics:

- Manual execution
- Windows Task Scheduler or cron
- GitHub Actions
- Dagster
- Airflow
- Scheduling
- Dependencies
- Retries
- Backfills
- Logs
- Monitoring
- Secrets in deployment environments

Acceptance criteria:

- Schedule a local pipeline.
- Add retry behavior.
- Run a backfill intentionally.
- Inspect execution logs.
- Explain when cron, Dagster, or Airflow is appropriate.

## Phase 16 — Production Capstone

Build one end-to-end pipeline:

```text
REST API + PostgreSQL + Files
              |
             dlt
              |
Normalization + Incremental + Merge + SCD Type 2
              |
Warehouse or Lakehouse
              |
Quality Checks + Alerts
              |
Airflow or Dagster
```

The capstone must include:

- At least one API source.
- A local PostgreSQL source.
- A file source.
- Normalized nested data.
- Incremental loading.
- Merge logic with keys.
- A backfill procedure.
- SCD Type 2 for one dimension.
- Schema-change handling.
- Data quality checks.
- Production-style logging and alerting.
- Deployment through an orchestrator.

## Current Next Step

Start Phase 2 with a small CSV file and a clear business key. Do not add APIs or PostgreSQL until the file ingestion acceptance criteria are complete.
