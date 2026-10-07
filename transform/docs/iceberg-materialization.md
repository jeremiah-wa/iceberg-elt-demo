# Writing Iceberg tables from dbt-core

Every model in this project uses a custom materialization, `iceberg_table` ([macros/iceberg_table.sql](../macros/iceberg_table.sql)), instead of dbt's built-in `table`. This page explains why. It covers what dbt-duckdb's `table` materialization does, which of its steps the DuckDB Iceberg extension rejects, what the workaround does instead and what it costs, and what would let us delete it.

The behavior described here was tested on 2026-10-07 with DuckDB 1.5.6, dbt-core 1.12.5, dbt-duckdb 1.11.0 and Lakekeeper v0.13.6, inside the Dagster container. [How this was tested](#how-this-was-tested) has the details.

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

The page also lists two write-compat catalog options, `stage_create_tables` and `disable_multi_table_commit`, which need DuckDB 1.5.4 or later. Their names suggest they work around the transaction limits described below, but the page doesn't explain what they do.

We can't switch. dagster-dbt only supports dbt-core 1.x. Support for dbt v2 is an open request, [dagster#34233](https://github.com/dagster-io/dagster/issues/34233). [pyproject.toml](../pyproject.toml) pins `dbt-core>=1.12,<1.13` for this reason, and the [README](../README.md#why-dbt-core-and-not-dbt-v2) has the details.

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

The `CASCADE` comes from dbt-duckdb's `duckdb__drop_relation` macro, which appends `cascade` to every drop. It has a special case that leaves `cascade` off for DuckLake catalogs, but none for Iceberg. The build fails at step 4 before it reaches step 7. Step 1 only drops something when a leftover tmp or backup table exists.

All of these errors come from DuckDB, not from Lakekeeper. Three of them are "Not implemented" errors, so newer versions of the Iceberg extension may lift them. Recheck when DuckDB is upgraded.

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

## When we can delete it

Either of two changes could make the macro unnecessary.

1. **dagster-dbt supports dbt v2.** Then move the `ATTACH` from `profiles.yml` into a `catalogs.yml` entry of type `iceberg_rest`, set `catalog_name` on the models, enable `use_catalogs_v2`, and go back to `materialized: table` (or `incremental` where it helps). Test a build against Lakekeeper first, and try the `stage_create_tables` and `disable_multi_table_commit` options if the built-in materialization hits the same errors.
2. **The DuckDB Iceberg extension allows renaming a table created in the same transaction.** Then `materialized: table` on dbt-core 1.x gets past step 4. Rebuilds would still hit `CASCADE` at step 7 unless the extension accepts it or dbt-duckdb leaves it off for Iceberg, as it does for DuckLake. To check, switch one model to `materialized: table` and run `dbt build --select <model>` twice. The first run tests the tmp-table rename, and the second tests the backup rename and drop.

After either change, delete [macros/iceberg_table.sql](../macros/iceberg_table.sql) and its entry in [macros/_macros.yml](../macros/_macros.yml), and update the "How it writes Iceberg" section of the [README](../README.md).

## How this was tested

All tests ran in the `dagster` container (`docker compose exec dagster ...`), so they used the locked DuckDB version and reached Lakekeeper at `lakekeeper:8181`. Test tables were dropped afterwards.

- **Statement table.** A Python script ran each statement through `duckdb` against a scratch namespace `lake.verify_scratch`. The same script checked the missing-table window (a second connection queried the table between the drop commit and the create), the failed-create case (a `CREATE TABLE ... AS` that raises after the drop commit), and the snapshot count from `iceberg_snapshots()` before and after a drop and recreate.
- **dbt behavior.** A throwaway dbt project with this repo's macros and the same `attach` config ran three models twice: a SQL model with `materialized='table'`, a SQL model with `materialized='iceberg_table'`, and a Python model with `materialized='iceberg_table'`. The built-in table failed on both runs with the rename error above. The `iceberg_table` model succeeded on both, which covers creating a new table and rebuilding an existing one. The Python model failed with the language error.
