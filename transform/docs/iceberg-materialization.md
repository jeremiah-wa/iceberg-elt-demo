# Writing Iceberg tables from dbt-core

Every model in this project uses dbt-duckdb's built-in `table` materialization. Two of the macros it calls are overridden in [macros/iceberg_relations.sql](../macros/iceberg_relations.sql), because DuckDB's Iceberg extension rejects the SQL that dbt-duckdb's versions generate. The overrides are a port of [dbt-duckdb#747](https://github.com/duckdb/dbt-duckdb/pull/747), which isn't merged. This page covers what the `table` materialization does, which of its steps the Iceberg extension rejects, what the overrides change, and what a rebuild still costs.

Until 2026-10-08 the models used a custom `iceberg_table` materialization instead. It dropped the table, committed and created it again, so the table was missing for the whole build. [What iceberg_table did](#what-iceberg_table-did) compares the two.

Everything here was tested inside the Dagster container with DuckDB 1.5.6, dbt-core 1.12.5, dbt-duckdb 1.11.0 and Lakekeeper v0.13.6. The statement table dates from 2026-10-07, the overrides from 2026-10-08. [How this was tested](#how-this-was-tested) has the details. The notes on dbt v2, upstream fixes and other adapters come from research on 2026-10-08 in source code, docs and issue trackers, and weren't tested. [iceberg-adapter-options.md](iceberg-adapter-options.md) has the full sources.

## How the project reaches Iceberg

There is no Iceberg-specific dbt config here. dbt-duckdb sees an ordinary attached DuckDB database.

- [profiles.yml](../profiles.yml) loads the `iceberg` extension and attaches the Lakekeeper warehouse `demo` as `lake` through the adapter's `attach` list. That's the same as running `ATTACH 'demo' AS lake (TYPE iceberg, ENDPOINT 'http://lakekeeper:8181/catalog', AUTHORIZATION_TYPE 'none')`.
- [dbt_project.yml](../dbt_project.yml) sets `+database: lake` and `+materialized: table` for every model.

Any `CREATE TABLE lake.staging.x` therefore goes through the Iceberg extension to Lakekeeper. dbt doesn't know this. Apart from the two overrides, the adapter treats `lake` the same as any other DuckDB database.

## Why not dbt's own Iceberg support

dbt documents DuckDB Iceberg support in [DuckDB and Apache Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/duckdb-iceberg-support). That support is for dbt v2 only, behind the `use_catalogs_v2` flag. The page says it isn't available in the Python `dbt-duckdb` adapter for dbt v1. With it you would:

- declare the catalog in `catalogs.yml` (type `iceberg_rest`, with a `duckdb:` block holding `endpoint` and an optional `secret`), and let dbt run the `ATTACH`;
- set `catalog_name` on each model. DuckDB has no built-in managed Iceberg catalog, so there is no `table_format='iceberg'` shortcut like Snowflake or Databricks have;
- use the built-in `table` or `incremental` materializations, the only two the page lists for Iceberg.

The page also lists write-compat catalog options such as `stage_create_tables` and `disable_multi_table_commit`. They're DuckDB `ATTACH` options for catalogs that reject an endpoint DuckDB uses by default, such as staged table creation or multi-table commits ([DuckDB Iceberg options](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_options.html)). None of them lifts the limits described below. dbt v1 can already pass them through the `attach` options in profiles.yml.

Switching would make rebuilds worse. dbt v2 runs into the same DuckDB limits and works around them by not renaming at all. For a catalog with `table_format: iceberg`, it picks a "direct create" write strategy. The source comment says this "skips the temp-table + rename dance entirely, since Iceberg REST attachments do not support `ALTER ... RENAME`" ([catalog_relation.rs](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-adapter/src/catalog_relation.rs#L21-L44)). The `table` materialization then drops the target before building it ([table.sql](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/materializations/table.sql#L22-L24)). On Iceberg that's `drop table if exists` without `CASCADE`, run outside a transaction, followed by a create straight into the final name ([adapters.sql](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql#L227-L238)). That's drop, commit, create, commit, the sequence `iceberg_table` ran, so the table would be missing for the whole build again. v2 would add built-in incremental models. This is read from the v2.0.5 source, not run.

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

Without the overrides, a model using `materialized='table'` against `lake` fails on its very first run, at step 4:

```
Runtime Error in model builtin_table (models/builtin_table.sql)
  Catalog Error: This table (builtin_table__dbt_tmp) was modified already, can't be renamed!
```

Steps 2 and 4 always happen, so there's no run on which the unmodified materialization works. Here is each statement it uses, and the obvious alternatives, run directly against Lakekeeper:

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

All of these errors come from DuckDB, not from Lakekeeper, and no DuckDB release lifts them yet. As of 2026-10-07, all four are still in the Iceberg extension's source, both on the branch DuckDB 1.5.6 is built from and on `main`. On `main` the rename rule is stricter. A transaction can hold table changes or a single rename or drop, but not both. The feature requests that would help are open: `CREATE OR REPLACE` ([duckdb-iceberg#784](https://github.com/duckdb/duckdb-iceberg/issues/784)), a single-snapshot `INSERT OVERWRITE` ([#620](https://github.com/duckdb/duckdb-iceberg/issues/620)) and `CASCADE` ([#1000](https://github.com/duckdb/duckdb-iceberg/issues/1000)). Recheck when DuckDB 2.0 ships, which the [release calendar](https://duckdb.org/release_calendar) plans for 2026-10-21.

## What the overrides change

[macros/iceberg_relations.sql](../macros/iceberg_relations.sql) changes two dbt-duckdb macros the way [#747](https://github.com/duckdb/dbt-duckdb/pull/747) does. dbt dispatches both and looks in the root project first, so the `table` materialization calls this project's versions instead of dbt-duckdb's.

- `duckdb__drop_relation` leaves `cascade` off drops in an Iceberg attachment, as dbt-duckdb already does for DuckLake. That fixes steps 1 and 7.
- `duckdb__rename_relation` commits the open transaction before it renames a table in an Iceberg attachment. That fixes step 4. The tmp table is committed before its rename, so the rename no longer shares a transaction with the `CREATE TABLE ... AS` that made it.

Both find Iceberg attachments with the `is_iceberg_relation` macro, which reads the target's `attach` list. #747 adds a Python adapter method for this instead, which a dbt project can't do. The macro also reads `type: iceberg` from under `options`, where [profiles.yml](../profiles.yml) sets it. #747's check only reads an attachment's top-level `type`, so as written it wouldn't treat this project's `lake` as Iceberg.

#747 also switches `duckdb__get_columns_in_relation` to `DESCRIBE`. dbt-duckdb 1.11.0 already does that, so it isn't ported.

With the overrides, rebuilding an existing model runs these transactions:

1. `BEGIN`, `CREATE TABLE <model>__dbt_tmp AS <model SQL>`, `COMMIT`. The new table now exists in Lakekeeper under the tmp name.
2. `BEGIN`, `ALTER TABLE <model> RENAME TO <model>__dbt_backup`, `COMMIT`. The model's name is now free.
3. `BEGIN`, `ALTER TABLE <model>__dbt_tmp RENAME TO <model>`, then post-hooks, grants and `persist_docs`, then `COMMIT`.
4. `DROP TABLE IF EXISTS <model>__dbt_backup`, without `CASCADE`, outside a transaction.

DuckDB sends each rename to Lakekeeper when its transaction commits. On a model's first build, when its table doesn't exist yet, step 2 doesn't happen.

## What it costs

**The table is missing for a moment.** Between the commits of steps 2 and 3, no table has the model's name. A query from another connection in that window fails with `Catalog Error: Table with name ... does not exist!`. The window covers the second rename and whatever runs before the final commit, which is nothing for the current models. In dbt's and Lakekeeper's logs it lasted 30 to 50 ms. A reader polling every 20 ms lost the table for under 0.1 s per rebuild. With `iceberg_table`, the same reader lost it for the whole `CREATE TABLE ... AS`.

**A failure in the model's SQL keeps the old table.** If the `CREATE TABLE ... AS` in step 1 fails, its transaction rolls back before anything else has changed. Readers keep seeing the old table, and nothing is left behind.

**A failure after step 2 leaves the table missing.** Step 3 rolls back if a post-hook, `persist_docs` or the rename itself fails. The old rows then sit in `<model>__dbt_backup` and the new ones in `<model>__dbt_tmp`, and nothing has the model's name. The table stays missing until the next successful run, which drops both leftovers before it builds. If that run fails too, the old rows are gone. No model here has post-hooks or `persist_docs`, so today only an error from Lakekeeper or the network during the renames can cause this.

**`persist_docs` doesn't work.** DuckDB rejects comments on Iceberg tables with `Not implemented Error: Only ALTER TABLE is supported for Iceberg`. Because `persist_docs` runs in step 3, a model that enables it loses its table on every build, as described above. Don't enable it.

**History still restarts on every run.** The table that ends up under the model's name is the new tmp table, with fresh metadata and one snapshot. Iceberg time travel never reaches past the latest build.

**Dropped tables' files stay in MinIO.** DuckDB drops tables with `purgeRequested=false`, and Lakekeeper removes the table from the catalog without deleting its files. Each rebuild leaves the backup table's files behind, about four objects per model. `iceberg_table` did the same.

**Grants and partitioning are ignored, with a warning.** dbt-duckdb warns and skips `grants`, which DuckDB doesn't support, and `partitioned_by` and `sorted_by`, which it only applies to DuckLake. `iceberg_table` ignored them silently.

**Python models work.** A throwaway Python model built and rebuilt as an Iceberg table. `iceberg_table` rejected Python models.

**Incremental models are untested.** dbt-duckdb's `incremental` materialization only renames tables on a full refresh, where the overrides apply the same way. Its normal runs change the table in place with the model's strategy, which nobody has tried on Iceberg here.

**The overrides shadow dbt-duckdb.** They replace dbt-duckdb's versions of both macros for every relation, not only Iceberg ones. If a dbt-duckdb release changes either macro, this project keeps running its own copy. Compare the two macros when upgrading dbt-duckdb.

## What iceberg_table did

The first workaround was a custom materialization, `iceberg_table`. It ran the only sequence in the [statement table](#where-it-breaks-on-iceberg) that worked without overrides: `drop table if exists <model>` without `cascade`, `COMMIT`, `CREATE TABLE <model> AS <model SQL>`, `COMMIT`. There was no tmp table and no rename.

The table was missing for the whole `CREATE TABLE ... AS`, and a build that failed there left no table at all, until a later run succeeded. History restarted on every run, as it still does. The macro also skipped grants, `persist_docs`, indexes and partitioning without a warning, and rejected Python models with `Materialization "materialization_iceberg_table_duckdb" only supports languages ['sql']; got "python"`. It was deleted when the models moved to `table`.

## When to delete the overrides

Delete [macros/iceberg_relations.sql](../macros/iceberg_relations.sql) and its entries in [macros/_macros.yml](../macros/_macros.yml) after either of these:

- A dbt-duckdb release includes #747 or an equivalent fix. If the release only reads an attachment's top-level `type`, as #747 does, move `type: iceberg` out of `options` in [profiles.yml](../profiles.yml) at the same time.
- DuckDB's Iceberg extension accepts `CASCADE` and renaming a table in the transaction that created it.

Then run `dbt build` twice against Lakekeeper. The first run creates the tables and the second rebuilds them.

## Other options

The options below would go further than the overrides, with no gap during a rebuild and with snapshot history kept. [iceberg-adapter-options.md](iceberg-adapter-options.md) compares them in detail. None of them has been tested against Lakekeeper.

1. **Replace rows in place.** A custom materialization would build the model into a local DuckDB temporary table, then run `DELETE FROM <model>` and `INSERT INTO <model> SELECT * FROM <temp>` in one transaction. Only one table changes and nothing is renamed, so none of the limits above apply, and the table keeps its snapshot history. When a model's columns change, it would fall back to drop and create. Whether readers can ever see an empty table depends on DuckDB sending the delete and the insert as one Iceberg commit, which nobody has tested. Every run also leaves position delete files and new snapshots behind. Nothing in this stack compacts tables, and open-source Lakekeeper doesn't expire snapshots, since that's a Lakekeeper Plus feature.
2. **Switch to an adapter whose engine replaces tables atomically.** dbt-trino with `on_table_exists='replace'` runs `CREATE OR REPLACE TABLE`, which Trino documents as atomic on Iceberg and which keeps history. dbt-spark with `file_format='iceberg'` runs Iceberg's replace-table-as-select, which is also atomic and keeps history. Both support dbt-core 1.12, so dagster-dbt stays as it is. Each adds a JVM service to the stack, and the models would have to move to that engine's SQL dialect.
3. **Wait for DuckDB.** `CREATE OR REPLACE` ([duckdb-iceberg#784](https://github.com/duckdb/duckdb-iceberg/issues/784)) or a single-snapshot `INSERT OVERWRITE` ([#620](https://github.com/duckdb/duckdb-iceberg/issues/620)) would let a materialization replace a table in one commit and keep its history. dbt-duckdb would then have to use them for Iceberg.

## How this was tested

All tests ran in the `dagster` container (`docker compose exec dagster ...`), so they used the locked DuckDB version and reached Lakekeeper at `lakekeeper:8181`. Test tables were dropped afterwards.

- **Statement table, 2026-10-07.** A Python script ran each statement through `duckdb` against a scratch namespace `lake.verify_scratch`. The same script checked `iceberg_table`'s missing-table window (a second connection queried the table between the drop commit and the create), the failed-create case, and the snapshot count from `iceberg_snapshots()` before and after a drop and recreate. A throwaway dbt project ran a model with `materialized='table'`, without the overrides, twice. Both runs failed with the rename error above.
- **Overrides, 2026-10-08.** While `dbt build` ran, a second DuckDB connection ran `select count(*)` on the model's table every 20 ms and logged each failure. To make the window measurable, `fct_pull_requests` was temporarily slowed to a 6 s `CREATE TABLE ... AS`.
  - With `iceberg_table`, the reader lost the table for 6.7 s.
  - With `table` and the overrides, two rebuilds in a row passed, and the reader lost the table for 0.09 s and 0.06 s.
  - A model whose SQL raised an error after 6 s failed the build. The reader saw the old rows throughout, and no tmp or backup table was left.
  - A post-hook that raised an error failed the build after the backup rename. It left `fct_pull_requests__dbt_tmp` and `fct_pull_requests__dbt_backup`, and nothing named `fct_pull_requests`. The next successful run dropped both and built the table.
  - `persist_docs: {relation: true, columns: true}` failed the same way, twice in a row. The second run dropped the first run's leftovers, the old rows in the backup included, before failing again.
  - `dbt build` of all models passed on empty schemas, then again over the existing tables. A throwaway Python model built and rebuilt.
  - dbt's debug log showed each `COMMIT`, and Lakekeeper's request log showed the two renames arriving during the commits, then the drop of the backup.
