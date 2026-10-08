# Writing Iceberg tables from dbt-core

Every model in this project is built by Trino, through the dbt-trino adapter, with dbt's `table` materialization and `on_table_exists: replace`. dbt-trino then rebuilds each table with one `CREATE OR REPLACE TABLE ... AS` statement, and Trino's Iceberg connector commits the result as a new snapshot of the existing table. This page covers what a rebuild runs, what it gives and costs, why the project moved off dbt-duckdb, and how it was tested.

Until 2026-10-08 the models ran on DuckDB, with a custom `iceberg_table` materialization that dropped each table and created it again. [Why not dbt-duckdb](#why-not-dbt-duckdb) explains why that changed.

Everything here was tested on 2026-10-08 with Trino 483, dbt-core 1.12.5, dbt-trino 1.10.6 and Lakekeeper v0.13.6, except the DuckDB statement table, which dates from 2026-10-07. [How this was tested](#how-this-was-tested) has the details. The notes on dbt v2 and other adapters come from research on 2026-10-08 and weren't tested. [iceberg-adapter-options.md](iceberg-adapter-options.md) has the sources.

## How the project reaches Iceberg

- [profiles.yml](../profiles.yml) points dbt-trino at the `trino` service on port 8080, with `lake` as the database. Trino has no login in this demo (`method: none`).
- Trino's catalog `lake`, configured in [infra/trino/catalog/lake.properties](../../infra/trino/catalog/lake.properties), is an Iceberg REST catalog for the Lakekeeper warehouse `demo`. Trino takes the S3 credentials Lakekeeper vends for each table, so the file only sets MinIO's endpoint, region and path-style access.
- [dbt_project.yml](../dbt_project.yml) sets `+database: lake`, `+materialized: table` and `+on_table_exists: replace` for every model.

A model's `CREATE TABLE lake.staging.x` runs in Trino. Trino commits the table to Lakekeeper and writes its files to MinIO. dbt only sends SQL to Trino over HTTP and reads back the results.

## What a rebuild runs

dbt-trino's `table` materialization picks its strategy from `on_table_exists` ([table.sql](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/dbt/include/trino/macros/materializations/table.sql)). With `replace`, a model's first build and every rebuild run the same single statement:

```sql
create or replace table lake.marts.fct_pull_requests as
select ...
```

Trino runs the query, writes new Parquet files to MinIO, and commits a replace transaction on the existing table ([Trino: replacing tables](https://trino.io/docs/current/connector/iceberg.html)). Lakekeeper receives one commit request for the table, with no drop, create or rename. The commit adds a snapshot that holds only the new rows and makes it current. Readers see the old snapshot until that commit, and the new one after it.

dbt-trino runs in autocommit mode, so the statement commits as soon as it finishes. Post-hooks and `persist_docs` run afterwards as separate statements.

The other `on_table_exists` values don't suit Iceberg, according to dbt-trino's source. The default, `rename`, builds `<model>__dbt_tmp`, then renames the old table away and the new one into place, so the table is missing between the two renames and its history restarts. `drop` drops the table and creates it again, as `iceberg_table` did.

## What it gives

**No gap.** Readers see the old table until the commit, then the new one. A DuckDB connection polled the table every 20 ms through a 6.6 s rebuild and never got an error. It switched from the old row count to the new one at the commit.

**A failed build keeps the old table.** If the query fails, Trino commits nothing. The table, its rows and its history stay as they were, and nothing is left behind.

**A failure after the commit keeps the new table.** A failing post-hook fails the model in dbt, and dbt skips its downstream models and tests. The table already holds the new rows, though.

**History is kept.** Every rebuild adds a snapshot to the same table, so time travel reaches earlier builds. `lake.marts."fct_pull_requests$snapshots"` lists the snapshots, and `select * from lake.marts.fct_pull_requests for version as of <snapshot_id>` reads one.

**`persist_docs` works.** Trino writes table and column comments into the Iceberg table. No model enables it yet.

## What it costs

**A JVM service.** Trino runs in its own container, capped at 3 GB of memory. It used about 1.9 GB in testing, and its image takes 2.4 GB of disk.

**Snapshots and files pile up.** Each rebuild adds about five files per model, and every earlier snapshot keeps its files. Nothing expires snapshots automatically, and open-source Lakekeeper doesn't either. Trino can:

```sql
alter table lake.marts.fct_pull_requests execute expire_snapshots(retention_threshold => '7d')
```

Seven days is the shortest retention Trino accepts unless the session sets `lake.expire_snapshots_min_retention` lower. In testing, expiring every snapshot but the current one took a table from 9 snapshots to 1 and deleted 30 files from MinIO.

**The models use Trino's SQL.** The models were written for DuckDB, and two things changed. Trino's `||` only joins strings, so the ids cast `number` to `varchar`. Trino's `date_diff('hour', ...)` counts whole hours, where DuckDB's counted hour boundaries, so `hours_to_close` can be one lower than it was.

**No Python models.** dbt-trino only runs SQL models.

**The docs site has no column types.** CI has no Trino, so `dbt docs generate` runs with `--empty-catalog` and `--no-populate-cache` and never connects. The catalog was empty there before too, because the old `docs` target attached an empty in-memory database.

## Why not dbt-duckdb

dbt-duckdb's `table` materialization builds `<model>__dbt_tmp`, renames the old table to `<model>__dbt_backup`, renames the new one into place and drops the backup, all in one transaction. DuckDB's Iceberg extension rejects several of those statements.

### Where it breaks on Iceberg

These statements were run directly against Lakekeeper with DuckDB 1.5.6:

| Statement | Result |
|---|---|
| `CREATE TABLE t ...` then `ALTER TABLE t RENAME ...` in one transaction | `Catalog Error: This table (t1) was modified already, can't be renamed!` |
| `ALTER TABLE t RENAME ...` on a table committed earlier | Works |
| `DROP TABLE IF EXISTS t CASCADE`, which dbt-duckdb sends for every drop | `Not implemented Error: DROP TABLE <table_name> CASCADE is not supported for Iceberg tables currently` |
| `DROP TABLE IF EXISTS t` without `CASCADE` | Works |
| `CREATE OR REPLACE TABLE t AS ...` on an existing table | `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements` |
| `DROP TABLE t` then `CREATE TABLE t AS ...` in one transaction | `Not implemented Error: Cannot create table deleted within a transaction: lake.verify_scratch.t5` |
| `DROP TABLE t`, `COMMIT`, then `CREATE TABLE t AS ...`, `COMMIT` | Works |

As of 2026-10-07 none of these limits is lifted on the extension's `main` branch. The feature requests are open: `CREATE OR REPLACE` ([duckdb-iceberg#784](https://github.com/duckdb/duckdb-iceberg/issues/784)), a single-snapshot `INSERT OVERWRITE` ([#620](https://github.com/duckdb/duckdb-iceberg/issues/620)) and `CASCADE` ([#1000](https://github.com/duckdb/duckdb-iceberg/issues/1000)).

So the project used a custom `iceberg_table` materialization, which ran the last row of the table: drop, commit, create, commit. While a model rebuilt, its table didn't exist, a build that failed left no table, and history restarted on every run.

### The DuckDB-based alternatives

They all fall short of Trino's replace:

- **Port [dbt-duckdb#747](https://github.com/duckdb/dbt-duckdb/pull/747).** The unmerged PR overrides dbt-duckdb's drop macro to leave off `CASCADE` and its rename macro to commit first, which lets the built-in `table` materialization run. Tested on 2026-10-08, rebuilds left the table missing for under 0.1 s, between the two renames. A failure in the model's SQL kept the old table, but a failure after the first rename, such as a failing post-hook, left the table missing until the next successful run. `persist_docs` failed, because DuckDB rejects comments on Iceberg tables, and history still restarted on every run.
- **Replace rows in place** with `DELETE` and `INSERT` in one transaction. That would keep history if DuckDB sends both as one Iceberg commit, which nobody has tested.
- **dbt v2.** Its DuckDB adapter avoids the renames by dropping the table, committing and creating it again, the same as `iceberg_table` ([catalog_relation.rs](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-adapter/src/catalog_relation.rs#L21-L44), [table.sql](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/materializations/table.sql#L22-L24)). dagster-dbt doesn't support v2 yet either ([dagster#34233](https://github.com/dagster-io/dagster/issues/34233)).

[iceberg-adapter-options.md](iceberg-adapter-options.md) compares these with dbt-spark and other adapters.

## How this was tested

dbt ran in the `dagster` container (`docker compose exec dagster ...`) and Trino in its own container, both on the Compose network with Lakekeeper and MinIO.

- **Trino, 2026-10-08.**
  - `dbt build` passed twice for all models. The first run replaced the tables DuckDB had built, and they kept their earlier snapshots.
  - While dbt rebuilt `fct_pull_requests`, a DuckDB 1.5.6 connection in a separate container ran `select count(*)` on it every 20 ms and logged each failure. A count over `tpch.sf10.lineitem` temporarily slowed the model to 6.6 s. No query failed. When a rebuild changed the row count from 358 to 175, the reader switched at the commit.
  - A model whose SQL raised an error after 6.6 s failed the build. The reader saw the old rows throughout, and the table gained no snapshot.
  - A post-hook that raised an error failed the model, but the table already had the new rows.
  - `persist_docs: {relation: true, columns: true}` wrote table and column comments on two runs in a row.
  - Lakekeeper's request log showed one commit for each rebuilt table, and no drops, creates or renames.
  - With `lake.expire_snapshots_min_retention` set to `0s` for the session, `expire_snapshots` with a `0s` threshold took `fct_pull_requests` from 9 snapshots to 1 and deleted 30 files.
  - A Dagster run materialized all models and evaluated their tests as asset checks.
  - In a Linux container with its network disconnected, the CI workflow's `dbt docs generate` command wrote the docs site after `uv sync --frozen --package transform`.
- **DuckDB, 2026-10-07.** A Python script in the `dagster` container ran each statement in the table above through `duckdb` against a scratch namespace `lake.verify_scratch`. A throwaway dbt project showed that dbt-duckdb's `table` materialization fails on the rename error, and that `iceberg_table` rebuilt tables with a gap.
- **dbt-duckdb#747, 2026-10-08.** The two overrides ran against Lakekeeper with the same 20 ms reader, on a separate branch. The results are summarized in [Why not dbt-duckdb](#why-not-dbt-duckdb).
