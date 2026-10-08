# Writing Iceberg tables from dbt-core

Every model in this project uses a custom materialization, `iceberg_table` ([macros/iceberg_table.sql](../macros/iceberg_table.sql)), instead of dbt's built-in `table`. This page explains why. It covers what dbt-duckdb's `table` materialization does, which of its steps the DuckDB Iceberg extension rejects, what the workaround does instead and what it costs, and the ways to replace it.

The behavior described here was tested on 2026-10-07 with DuckDB 1.5.6, dbt-core 1.12.5, dbt-duckdb 1.11.0 and Lakekeeper v0.13.6, inside the Dagster container. [How this was tested](#how-this-was-tested) has the details. The notes on dbt v2, upstream fixes and other adapters come from research on 2026-10-08 in source code, docs and issue trackers, and weren't tested. [iceberg-adapter-options.md](iceberg-adapter-options.md) has the full sources.

## How the project reaches Iceberg

There is no Iceberg-specific dbt config here. dbt-duckdb sees an ordinary attached DuckDB database.

- [profiles.yml](../profiles.yml) loads the `iceberg` extension and attaches the Lakekeeper warehouse `demo` as `lake` through the adapter's `attach` list. That's the same as running `ATTACH 'demo' AS lake (TYPE iceberg, ENDPOINT 'http://lakekeeper:8181/catalog', AUTHORIZATION_TYPE 'none')`.
- [dbt_project.yml](../dbt_project.yml) sets `+database: lake` and `+materialized: iceberg_table` for every model.

Any `CREATE TABLE lake.staging.x` therefore goes through the Iceberg extension to Lakekeeper. dbt doesn't know this. The adapter treats `lake` the same as any other DuckDB database.

## Why not dbt's own Iceberg support

dbt documents DuckDB Iceberg support in [DuckDB and Apache Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/duckdb-iceberg-support). That support is for dbt v2 only, behind the `use_catalogs_v2` flag. The page says it isn't available in the Python `dbt-duckdb` adapter for dbt v1. With it you would:

- declare the catalog in `catalogs.yml` (type `iceberg_rest`, with a `duckdb:` block holding `endpoint` and an optional `secret`), and let dbt run the `ATTACH`;
- set `catalog_name` on each model. DuckDB has no built-in managed Iceberg catalog, so there is no `table_format='iceberg'` shortcut like Snowflake or Databricks have;
- use the built-in `table` or `incremental` materializations, the only two the page lists for Iceberg.

The page also lists write-compat catalog options such as `stage_create_tables` and `disable_multi_table_commit`. They're DuckDB `ATTACH` options for catalogs that reject an endpoint DuckDB uses by default, such as staged table creation or multi-table commits ([DuckDB Iceberg options](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_options.html)). None of them lifts the limits described below. dbt v1 can already pass them through the `attach` options in profiles.yml.

Switching wouldn't fix rebuilds anyway. dbt v2 runs into the same DuckDB limits, and its DuckDB adapter works around them the way this project does. For a catalog with `table_format: iceberg`, it picks a "direct create" write strategy. The source comment says this "skips the temp-table + rename dance entirely, since Iceberg REST attachments do not support `ALTER ... RENAME`" ([catalog_relation.rs](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-adapter/src/catalog_relation.rs#L21-L44)). The `table` materialization then drops the target before building it ([table.sql](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/materializations/table.sql#L22-L24)). On Iceberg that's `drop table if exists` without `CASCADE`, run outside a transaction, followed by a create straight into the final name ([adapters.sql](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql#L227-L238)). That's drop, commit, create, commit, the same sequence as `iceberg_table`, with the same costs. v2 would add grants, `persist_docs` and built-in incremental models. This is read from the v2.0.5 source, not run.

We also can't switch yet. dagster-dbt 0.29.25 requires `dbt-core<1.13`, its dbt Fusion support is in preview, and dbt v2 support is an open request, [dagster#34233](https://github.com/dagster-io/dagster/issues/34233). [pyproject.toml](../pyproject.toml) pins `dbt-core>=1.12,<1.13` for this reason, and the [README](../README.md#why-dbt-core-and-not-dbt-v2) has the details.

## What the built-in `table` materialization does

dbt-duckdb 1.11's `table` materialization lives in `dbt/include/duckdb/macros/materializations/table.sql` inside the installed package. It runs, in order:

1. `drop_relation_if_exists` on leftover `<model>__dbt_tmp` and `<model>__dbt_backup` tables, if dbt's cache has any.
2. `BEGIN`, then `CREATE TABLE <model>__dbt_tmp AS <model SQL>`.
3. If the model's table already exists, `ALTER TABLE <model> RENAME TO <model>__dbt_backup`.
4. `ALTER TABLE <model>__dbt_tmp RENAME TO <model>`.
5. Indexes, inside-transaction post-hooks, grants and `persist_docs`.
6. `COMMIT`.
7. `drop_relation_if_exists(<model>__dbt_backup)`.

On a normal database this is a good design. Readers see the old table until the commit, then the new one, and a failed build leaves the old table in place.

## Where it breaks on Iceberg

A model using `materialized='table'` against `lake` fails on its very first run, at step 4:

```
Runtime Error in model builtin_table (models/builtin_table.sql)
  Catalog Error: This table (builtin_table__dbt_tmp) was modified already, can't be renamed!
```

Steps 2 and 4 always happen, so there's no run on which the built-in materialization works. Here is each statement it uses, and the obvious alternatives, run directly against Lakekeeper:

| Statement | Result |
|---|---|
| `CREATE TABLE t ...` then `ALTER TABLE t RENAME ...` in one transaction (steps 2 and 4) | `Catalog Error: This table (t1) was modified already, can't be renamed!` |
| `ALTER TABLE t RENAME ...` on a table committed earlier (step 3) | Works |
| `DROP TABLE IF EXISTS t CASCADE` (steps 1 and 7) | `Not implemented Error: DROP TABLE <table_name> CASCADE is not supported for Iceberg tables currently` |
| `DROP TABLE IF EXISTS t` without `CASCADE` | Works |
| `CREATE OR REPLACE TABLE t AS ...` on an existing table | `Not implemented Error: CREATE OR REPLACE not supported in DuckDB-Iceberg. Please use separate Drop and Create Statements` |
| `DROP TABLE t` then `CREATE TABLE t AS ...` in one transaction | `Not implemented Error: Cannot create table deleted within a transaction: lake.verify_scratch.t5` |
| `DROP TABLE t`, `COMMIT`, then `CREATE TABLE t AS ...`, `COMMIT` | Works |

The `CASCADE` comes from dbt-duckdb's `duckdb__drop_relation` macro, which appends `cascade` to every drop. It has a special case that leaves `cascade` off for DuckLake catalogs, but none for Iceberg. An open dbt-duckdb pull request, [#747](https://github.com/duckdb/dbt-duckdb/pull/747), adds one. The build fails at step 4 before it reaches step 7. Step 1 only drops something when a leftover tmp or backup table exists.

All of these errors come from DuckDB, not from Lakekeeper, and no DuckDB release lifts them yet. As of 2026-10-07, all four are still in the Iceberg extension's source, both on the branch DuckDB 1.5.6 is built from and on `main`. On `main` the rename rule is stricter. A transaction can hold table changes or a single rename or drop, but not both. The feature requests that would help are open: `CREATE OR REPLACE` ([duckdb-iceberg#784](https://github.com/duckdb/duckdb-iceberg/issues/784)), a single-snapshot `INSERT OVERWRITE` ([#620](https://github.com/duckdb/duckdb-iceberg/issues/620)) and `CASCADE` ([#1000](https://github.com/duckdb/duckdb-iceberg/issues/1000)). Recheck when DuckDB 2.0 ships, which the [release calendar](https://duckdb.org/release_calendar) plans for 2026-10-21.

## What `iceberg_table` does instead

The only sequence in the table above that works is drop, commit, create, commit, so that's what the macro does:

```sql
{% materialization iceberg_table, adapter='duckdb' %}
  -- 1. look up the existing table and build the target relation
  -- 2. pre-hooks (outside, then inside the transaction)
  -- 3. if the table exists: drop table if exists <model>; COMMIT
  -- 4. CREATE TABLE <model> AS <model SQL>
  -- 5. inside-transaction post-hooks; COMMIT; outside post-hooks
  -- 6. return the relation so dbt's cache knows it exists
{% endmaterialization %}
```

Step by step:

1. `load_cached_relation(this)` reads dbt's relation cache, which dbt fills from the catalog at the start of the run. If the model's table is already in `lake`, `existing_relation` is set.
2. Pre-hooks run in the same two phases as the built-in materialization.
3. If the table exists, the macro runs its own `drop table if exists {{ existing_relation }}` with no `cascade`. It doesn't call `drop_relation_if_exists()`, because that would add `cascade`. Then `adapter.commit()` puts the drop in its own transaction. Inside-transaction pre-hooks are committed along with it.
4. `create_table_as(False, target_relation, compiled_code)` writes straight to the final name. There's no temporary table and no rename.
5. Post-hooks run, then the second commit makes the new table visible.
6. Returning `{'relations': [target_relation]}` updates dbt's cache, so downstream models and tests in the same run find the table.

## What it costs

**The table is missing during a rebuild.** Between the first commit (drop) and the second (create), the table doesn't exist in Lakekeeper. A query from another connection during that window fails with `Catalog Error: Table with name ... does not exist!`, rather than returning the old data. The window lasts as long as the model's `CREATE TABLE ... AS` takes.

**A failed build leaves no table.** If the create fails because of bad SQL or a Lakekeeper or S3 error, the old table has already been dropped. It stays gone until a later run succeeds. dbt skips the downstream models and tests in that run.

**Iceberg history restarts on every run.** Each run creates a new table with fresh metadata. In testing, a table with two snapshots had one snapshot after a rebuild, so time travel never reaches past the latest build.

**It drops some features of the built-in materialization.** `iceberg_table` never calls the code for:

- `grants`
- `persist_docs` (column and table comments)
- `indexes`
- `partitioned_by` and `sorted_by`

None of the current models use them. If one is added to a model config, dbt won't apply it and won't warn.

Python models fail outright. The macro doesn't declare `supported_languages`, so dbt defaults it to `['sql']` and stops with `Materialization "materialization_iceberg_table_duckdb" only supports languages ['sql']; got "python"`.

**There's no incremental mode.** Every run rebuilds every table in full. That's fine for the current data size.

## Ways to replace it

dbt v2 isn't one of them, for the reason in [Why not dbt's own Iceberg support](#why-not-dbts-own-iceberg-support). [iceberg-adapter-options.md](iceberg-adapter-options.md) compares the options below in detail. None of them has been tested against Lakekeeper yet.

1. **Port dbt-duckdb PR #747.** [#747](https://github.com/duckdb/dbt-duckdb/pull/747) changes two macros. `duckdb__drop_relation` leaves off `CASCADE` for Iceberg, and `duckdb__rename_relation` commits before renaming an Iceberg table. The PR isn't merged, but dbt dispatches both macros and looks in the root project first, so copies in this project's `macros/` folder would override dbt-duckdb's. Models could then go back to `materialized: table`. Every statement in the resulting sequence works in the table above. The old table stays readable while the model's SQL runs, and a failed build leaves it in place. The table is missing only between the last rename and the final commit. History still restarts on every run.
2. **Rewrite `iceberg_table` to replace rows in place.** On later runs, the macro would build the model into a local DuckDB temporary table, then run `DELETE FROM <model>` and `INSERT INTO <model> SELECT * FROM <temp>` in one transaction. Only one table changes and nothing is renamed, so none of the limits above apply, and the table keeps its snapshot history. When a model's columns change, it would fall back to today's drop and create. Whether readers can ever see an empty table depends on DuckDB sending the delete and the insert as one Iceberg commit, which nobody has tested. Every run also leaves position delete files and new snapshots behind. Nothing in this stack compacts tables, and open-source Lakekeeper doesn't expire snapshots, since that's a Lakekeeper Plus feature.
3. **Switch to an adapter whose engine replaces tables atomically.** dbt-trino with `on_table_exists='replace'` runs `CREATE OR REPLACE TABLE`, which Trino documents as atomic on Iceberg and which keeps history. dbt-spark with `file_format='iceberg'` runs Iceberg's replace-table-as-select, which is also atomic and keeps history. Both support dbt-core 1.12, so dagster-dbt stays as it is. Each adds a JVM service to the stack, and the models would have to move to that engine's SQL dialect.
4. **Wait for DuckDB to allow renaming a table created in the same transaction.** Then `materialized: table` on dbt-core 1.x gets past step 4. Rebuilds would still hit `CASCADE` at step 7 unless the extension accepts it or dbt-duckdb leaves it off for Iceberg, as #747 does. To check, switch one model to `materialized: table` and run `dbt build --select <model>` twice. The first run tests the tmp-table rename, and the second tests the backup rename and drop.

Options 1, 3 and 4 make the macro unnecessary. After any of them, delete [macros/iceberg_table.sql](../macros/iceberg_table.sql) and its entry in [macros/_macros.yml](../macros/_macros.yml), and update the "How it writes Iceberg" section of the [README](../README.md). Option 2 keeps the macro and changes what it does.

## How this was tested

All tests ran in the `dagster` container (`docker compose exec dagster ...`), so they used the locked DuckDB version and reached Lakekeeper at `lakekeeper:8181`. Test tables were dropped afterwards.

- **Statement table.** A Python script ran each statement through `duckdb` against a scratch namespace `lake.verify_scratch`. The same script checked the missing-table window (a second connection queried the table between the drop commit and the create), the failed-create case (a `CREATE TABLE ... AS` that raises after the drop commit), and the snapshot count from `iceberg_snapshots()` before and after a drop and recreate.
- **dbt behavior.** A throwaway dbt project with this repo's macros and the same `attach` config ran three models twice: a SQL model with `materialized='table'`, a SQL model with `materialized='iceberg_table'`, and a Python model with `materialized='iceberg_table'`. The built-in table failed on both runs with the rename error above. The `iceberg_table` model succeeded on both, which covers creating a new table and rebuilding an existing one. The Python model failed with the language error.
