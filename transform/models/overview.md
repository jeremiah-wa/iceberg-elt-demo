{% docs __overview__ %}
# Iceberg ELT demo

This is the dbt project of [iceberg-elt-demo](https://github.com/jeremiah-wa/iceberg-elt-demo), an ELT pipeline that keeps every table in Apache Iceberg:

1. **Extract and load.** A dlt pipeline loads issues and pull requests of five open-source repos (apache/iceberg, dlt-hub/dlt, dbt-labs/dbt, dagster-io/dagster, lakekeeper/lakekeeper) from the GitHub REST API into `raw.issues` and `raw.pull_requests`.
2. **Transform.** This project cleans them into the `staging` models and builds the `marts` models on top.
3. **Orchestrate.** Dagster runs both steps as one job, daily at 06:00.

All tables are Iceberg tables: Parquet files in MinIO, registered in the Lakekeeper REST catalog (warehouse `demo`). DuckDB does the compute, through the dbt-duckdb adapter.

## Layers

| Schema | Models | Purpose |
|---|---|---|
| `raw` | dbt sources | GitHub API records as dlt loaded them, nested fields flattened with `__` |
| `staging` | `stg_github__*` | One model per source table: renamed columns, typed flags, consistent ids. No joins or aggregation |
| `marts` | `fct_*` | Tables for analysis, built from staging |

## Data scope

Each load takes the latest 100 items per repo, issues and pull requests together, ordered by creation time, and replaces the raw tables. The tables are a snapshot of recent activity, not full history. A repo with many recent pull requests has fewer issues in the snapshot, and the other way round.

## About this site

GitHub Actions builds this site without access to the catalog, so column data types are missing. Column descriptions come from the project's YAML files.
{% enddocs %}
