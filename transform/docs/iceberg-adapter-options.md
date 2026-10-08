# Options for atomic Iceberg rebuilds

> [!NOTE]
> Update, 2026-10-08. [Option 2](#option-2-keep-dbt-duckdb-commit-before-each-rename) was tested against Lakekeeper, and the models now use it. Rebuilds ran the sequence described there. The tests also settled two open questions. `persist_docs` fails on Iceberg tables, and any failure after the backup rename, not only a failed final commit, leaves the table missing until the next successful run. [iceberg-materialization.md](iceberg-materialization.md) has the results. The rest of this page is the research as written before the tests.

[iceberg-materialization.md](iceberg-materialization.md) explains why every model uses the custom `iceberg_table` materialization. DuckDB's Iceberg extension rejects the statements dbt-duckdb's `table` materialization runs, so the macro drops the table, commits and creates it again. While a model rebuilds, its table doesn't exist. A failed build leaves no table, and the table's Iceberg snapshot history restarts on every run.

This page compares other ways to build the tables. The goal is a rebuild that keeps the old table readable until the new one commits and, ideally, keeps snapshot history, against an Iceberg REST catalog like Lakekeeper. It covers fixes that keep dbt-duckdb, other dbt v1 adapters, and dbt v2.

Researched on 2026-10-08 from official docs, adapter source on GitHub, release notes and issue trackers. Nothing on this page was run against this repo's stack. Where a statement about runtime behavior comes from reading source code rather than a test, the text says so. [Not verified](#not-verified) collects the open questions.

## What to compare

Each option is judged on four things:

- **Atomic rebuild.** Readers see the old table until the new one commits, and a failed build leaves the old table in place.
- **Snapshot history.** A rebuild adds snapshots to the existing table instead of creating a new table.
- **dbt v1.** dagster-dbt 0.29.25 requires `dbt-core>=1.7,<1.13` ([PyPI](https://pypi.org/pypi/dagster-dbt/0.29.25/json)), so an adapter must work with dbt-core 1.12.
- **Cost to this repo.** New services in `infra/docker-compose.yml`, SQL changes, and how well the adapter is maintained.

## Option 1: keep dbt-duckdb, rebuild in place with DELETE and INSERT

This changes only [macros/iceberg_table.sql](../macros/iceberg_table.sql). dbt-duckdb, DuckDB and the compose stack stay as they are. The macro would:

1. Run `CREATE TABLE <model> AS ...` if the table doesn't exist yet, as it does now.
2. Otherwise build the model into a DuckDB temporary table, which lives in the local DuckDB session and not in Lakekeeper.
3. Compare the temporary table's columns with the Iceberg table's.
4. If they match, run `DELETE FROM <model>` and `INSERT INTO <model> SELECT * FROM <temp>` in one transaction, then commit.
5. If they don't match, fall back to today's drop, commit, create.

None of the four statements DuckDB rejects is involved. There's no rename, no `CASCADE`, no `CREATE OR REPLACE` and no create after a drop in the same transaction.

What the sources say:

- DuckDB's Iceberg extension supports `UPDATE` and `DELETE` "on both partitioned and unpartitioned tables", using merge-on-read and positional delete files. It also supports `MERGE INTO` ([DuckDB: Writing to Iceberg](https://duckdb.org/docs/current/core_extensions/iceberg/writing)).
- `UPDATE` and `DELETE` fail if a table sets `write.update.mode` or `write.delete.mode` to anything other than `merge-on-read` ([same page, Limitations](https://duckdb.org/docs/current/core_extensions/iceberg/writing)).
- On the extension's `main` branch, a transaction that changes more than one table needs the catalog's multi-table commit endpoint. A transaction that changes one table doesn't ([iceberg_transaction.cpp, `VerifyAlterUpdateAtomicity`](https://github.com/duckdb/duckdb-iceberg/blob/25509bdb99623d1894d139eb3bc5199d040ea4b2/src/catalog/rest/transaction/iceberg_transaction.cpp#L385-L399)). A DELETE and an INSERT on the same table should therefore reach Lakekeeper as one commit. This is inferred from `main`. The `v1.5-variegata` branch that DuckDB 1.5.6 uses wasn't checked for the same behavior.

What it costs:

- **Untested.** Whether readers see either the old rows or the new rows, never an empty table, depends on DuckDB sending the delete and the insert as one Iceberg commit. Nobody has tested that on DuckDB 1.5.6 against Lakekeeper.
- **Delete files pile up.** A merge-on-read `DELETE` keeps the old data files in the table and adds position delete files that mask their rows ([Iceberg spec: position delete files](https://iceberg.apache.org/spec/#position-delete-files)). DuckDB doesn't yet drop a data file when every row in it is deleted. The PR that would have done that was closed unmerged ([duckdb-iceberg#1178](https://github.com/duckdb/duckdb-iceberg/pull/1178)), and the follow-ups are open ([#1287](https://github.com/duckdb/duckdb-iceberg/pull/1287), [#1260](https://github.com/duckdb/duckdb-iceberg/issues/1260)). So after many rebuilds the current snapshot still references every earlier run's data files plus their delete files, until something compacts the table. DuckDB 1.5 has no compaction function. `iceberg_rewrite_data_files` exists only on the extension's `main` branch ([source](https://github.com/duckdb/duckdb-iceberg/blob/25509bdb99623d1894d139eb3bc5199d040ea4b2/src/function/metadata/iceberg_rewrite_data_files.cpp)).
- **Snapshots pile up.** Open-source Lakekeeper doesn't expire snapshots. The `expire_snapshots` task endpoints appear only in the Lakekeeper Plus management API spec ([management-open-api-plus.yaml](https://github.com/lakekeeper/lakekeeper/blob/69ec61f129761ce607027581ad68a0fce5df1898/docs/docs/api/management-open-api-plus.yaml)), not in the open-source one ([management-open-api.yaml](https://github.com/lakekeeper/lakekeeper/blob/69ec61f129761ce607027581ad68a0fce5df1898/docs/docs/api/management-open-api.yaml)). Storage grows with every run unless another engine expires snapshots, for example Trino's `expire_snapshots` ([Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)).
- **Schema changes are still not atomic.** When a model's columns change, the fallback drops and recreates the table, with today's gap and history reset.
- **The other gaps stay.** The macro would still skip grants, `persist_docs`, `partitioned_by` and `sorted_by`, and wouldn't run Python models, unless that code is added.
- **The whole result is built locally first.** The temporary table holds the full model output in the Dagster container. That's fine at the current data size.

Incremental models could follow the same pattern with `MERGE INTO`, which DuckDB supports on Iceberg. That's not built either.

## Option 2: keep dbt-duckdb, commit before each rename

[dbt-duckdb PR #747](https://github.com/duckdb/dbt-duckdb/pull/747), "Fix Iceberg REST CTAS + rename transaction conflict", makes the built-in `table` materialization work against an Iceberg REST catalog with two macro changes ([diff](https://github.com/duckdb/dbt-duckdb/pull/747/files)):

- `duckdb__drop_relation` leaves off `CASCADE` for Iceberg attachments, as dbt-duckdb 1.11.0 already does for DuckLake ([adapters.sql L289-L297](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/dbt/include/duckdb/macros/adapters.sql#L289-L297)).
- `duckdb__rename_relation` commits the open transaction before renaming an Iceberg table.

The PR has been open since 2026-05-21, was last updated on 2026-06-28, and still needs review. Its test plan, including an integration test against Lakekeeper, isn't checked off.

The repo doesn't have to wait for a release. dbt's `dispatch` looks in the root project first ([dispatch config](https://docs.getdbt.com/reference/project-configs/dispatch-config)), and both macros are dispatched ([drop.sql](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-adapters/src/dbt/include/global_project/macros/relations/drop.sql#L26-L27), [rename.sql](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-adapters/src/dbt/include/global_project/macros/relations/rename.sql#L26-L27)). Copies of the two overridden macros in this project's `macros/` folder would apply to every model. Models could then go back to `materialized: table`.

Combined with dbt-duckdb 1.11.0's `table` materialization ([table.sql](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/dbt/include/duckdb/macros/materializations/table.sql)), a rebuild would run as follows. This sequence is read from the source, not tested.

1. `BEGIN`, then `CREATE TABLE <model>__dbt_tmp AS <model SQL>`.
2. Commit, so the tmp table exists. Then `ALTER TABLE <model> RENAME TO <model>__dbt_backup`.
3. Commit, so the backup rename takes effect and `<model>` no longer exists. Then `ALTER TABLE <model>__dbt_tmp RENAME TO <model>`.
4. Post-hooks, grants and `persist_docs`, then `COMMIT`.
5. `drop table if exists <model>__dbt_backup`, without `CASCADE`.

Every statement in that list is one that [iceberg-materialization.md](iceberg-materialization.md#where-it-breaks-on-iceberg) found working against Lakekeeper: `CREATE TABLE ... AS`, renaming a table committed earlier, and `DROP TABLE` without `CASCADE`.

What it gives and costs:

- The old table stays readable while the model's SQL runs. It's missing only between the commit in step 3 and the commit in step 4, which covers the rename request and the hooks.
- A failed `CREATE TABLE ... AS` leaves the old table untouched.
- **History still restarts.** The table that ends up under the model's name is the new tmp table.
- If the final commit fails, the old data sits under `<model>__dbt_backup` rather than the model's name. This is inferred from the sequence above.
- grants, `persist_docs` and `indexes` come back, because the built-in materialization calls them. `persist_docs` would issue `COMMENT` statements in the same transaction as the rename in step 3. Whether DuckDB allows that is unknown. On the extension's `main` branch, a transaction can't mix table updates with a rename ([source](https://github.com/duckdb/duckdb-iceberg/blob/25509bdb99623d1894d139eb3bc5199d040ea4b2/src/catalog/rest/transaction/iceberg_transaction.cpp#L1019-L1029)). No model here uses `persist_docs` today.
- The project would carry two overrides of dbt-duckdb internals until the PR merges, or for good if it doesn't.

## Fixing it upstream

### dbt-duckdb

- The latest release is 1.11.0, from 2026-08-07 ([releases](https://github.com/duckdb/dbt-duckdb/releases)). It requires `dbt-core>=1.8.0` ([PyPI](https://pypi.org/pypi/dbt-duckdb/json)).
- No release treats Iceberg attachments specially in `drop_relation` or `rename_relation` ([1.11.0 adapters.sql](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/dbt/include/duckdb/macros/adapters.sql#L289-L304)).
- Open work on Iceberg:
  - [#747](https://github.com/duckdb/dbt-duckdb/pull/747), described in [Option 2](#option-2-keep-dbt-duckdb-commit-before-each-rename).
  - [#725](https://github.com/duckdb/dbt-duckdb/pull/725), marked "DO NOT MERGE (yet)". It replaces CTAS with `CREATE TABLE` plus `INSERT` for Iceberg, to work around a CTAS problem under dbt v2's ADBC driver. #747 calls itself the simpler alternative.
  - [#833](https://github.com/duckdb/dbt-duckdb/pull/833), "Declare typed DuckDB table lifecycle planning". It depends on unreleased dbt-core and dbt-adapters changes.
  - [#755](https://github.com/duckdb/dbt-duckdb/pull/755), which stops `generate_schema_name` from prefixing schemas in attached Iceberg databases. This repo already overrides that macro ([generate_schema_name.sql](../macros/generate_schema_name.sql)).

### DuckDB's Iceberg extension

- DuckDB 1.5.6, from 2026-09-28, is the latest release. DuckDB 2.0.0 is planned for 2026-10-21, a date the calendar calls tentative ([release calendar](https://duckdb.org/release_calendar)).
- On the `v1.5-variegata` branch (last commit 2026-10-02), all four errors from [iceberg-materialization.md](iceberg-materialization.md#where-it-breaks-on-iceberg) are still in the source:
  - `CREATE OR REPLACE not supported` ([iceberg_schema_entry.cpp L47](https://github.com/duckdb/duckdb-iceberg/blob/5dcf5070c50341c2e4b4403b67ed4cbc7afaa37b/src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp#L47))
  - `Cannot create table deleted within a transaction` ([L62](https://github.com/duckdb/duckdb-iceberg/blob/5dcf5070c50341c2e4b4403b67ed4cbc7afaa37b/src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp#L62))
  - `DROP TABLE <table_name> CASCADE is not supported` ([L119](https://github.com/duckdb/duckdb-iceberg/blob/5dcf5070c50341c2e4b4403b67ed4cbc7afaa37b/src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp#L119))
  - `This table (...) was modified already, can't be renamed!` ([iceberg_transaction.cpp L819](https://github.com/duckdb/duckdb-iceberg/blob/5dcf5070c50341c2e4b4403b67ed4cbc7afaa37b/src/catalog/rest/transaction/iceberg_transaction.cpp#L819))
- On `main` (commit of 2026-10-07), the first three errors are unchanged ([L54, L69, L146](https://github.com/duckdb/duckdb-iceberg/blob/25509bdb99623d1894d139eb3bc5199d040ea4b2/src/catalog/rest/catalog_entry/schema/iceberg_schema_entry.cpp#L54)). The rename rule is stricter: a transaction holds either table updates or one rename or drop, and mixing them raises `cannot commit this transaction atomically because it mixes table updates with rename/drop requests` ([L1019-L1067](https://github.com/duckdb/duckdb-iceberg/blob/25509bdb99623d1894d139eb3bc5199d040ea4b2/src/catalog/rest/transaction/iceberg_transaction.cpp#L1019-L1067)). Whether 2.0.0 ships from this branch is not verified.
- `ALTER TABLE ... RENAME TO` support arrived in [#924](https://github.com/duckdb/duckdb-iceberg/pull/924) (merged 2026-04-16) with the stated limitation: "We don't allow creation of the table or other ALTER statements to happen in the same transaction as a RENAME of the table."
- Feature requests that would help are open:
  - [#784](https://github.com/duckdb/duckdb-iceberg/issues/784), `CREATE OR REPLACE TABLE`. A maintainer replied that it "does not sound trivial to implement", because Iceberg can't drop the old table until commit.
  - [#620](https://github.com/duckdb/duckdb-iceberg/issues/620), `INSERT OVERWRITE` as a single-snapshot overwrite.
  - [#1000](https://github.com/duckdb/duckdb-iceberg/issues/1000), allowing `CASCADE`. The maintainers asked for an example of another engine's behavior and said that without one "the chance we implement it is very small".

No DuckDB release after 1.5.6 exists yet. Nothing on the extension's `main` branch lifts any of the four limits.

## Option 3: dbt-trino

| | |
|---|---|
| Latest release | 1.10.6, 2026-10-02 ([CHANGELOG](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/CHANGELOG.md)) |
| dbt-core range | `dbt-core>=1.8.0,<2.0`, `dbt-adapters>=1.16,<2.0` ([setup.py](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/setup.py)) |
| Maintainer | Starburst ([dbt docs](https://docs.getdbt.com/docs/local/connect-data-platform/trino-setup)). Four releases between July and October 2026 |
| New service | One Trino container. The latest Trino is 483, from 2026-07-18 ([releases](https://github.com/trinodb/trino/releases)) |

**Connecting to Lakekeeper.** dbt-trino doesn't talk to the REST catalog. Trino does, through an Iceberg catalog with `iceberg.catalog.type=rest`, configured on the Trino server ([Trino metastores](https://trino.io/docs/current/object-storage/metastores.html)). The dbt `database` is the Trino catalog name. `iceberg.rest-catalog.vended-credentials-enabled=true` makes Trino use credentials from the catalog. It defaults to `false` ([same page](https://trino.io/docs/current/object-storage/metastores.html)). Lakekeeper's docs say no S3 credentials are needed with vended credentials, but S3-compatible storage still needs `s3.endpoint`, `s3.path-style-access` and `s3.region` set on the Trino catalog. They also recommend `iceberg.rest-catalog.nested-namespace-enabled`, and `iceberg.unique-table-location` when soft deletion is on ([Lakekeeper: Trino](https://docs.lakekeeper.io/docs/latest/engines/#trino)). Lakekeeper's minimal example creates exactly such a catalog against the same MinIO fork this repo uses ([Trino notebook](https://github.com/lakekeeper/lakekeeper/blob/69ec61f129761ce607027581ad68a0fce5df1898/examples/minimal/notebooks/Trino.ipynb), [compose file](https://github.com/lakekeeper/lakekeeper/blob/69ec61f129761ce607027581ad68a0fce5df1898/examples/minimal/docker-compose.yaml)).

**What `table` runs.** The `on_table_exists` config picks the strategy. The default is `rename` ([table.sql](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/dbt/include/trino/macros/materializations/table.sql)).

| `on_table_exists` | Statements | Atomic rebuild | History |
|---|---|---|---|
| `rename` (default) | CTAS `<model>__dbt_tmp`, rename `<model>` to `__dbt_backup`, rename tmp to `<model>`, drop backup | No. dbt-trino connects in autocommit mode ([connections.py L720](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/dbt/adapters/trino/connections.py#L720)), so the table is missing between the two renames. A failed CTAS leaves the old table | Restarts |
| `replace` | `CREATE OR REPLACE TABLE <model> AS ...` ([adapters.sql L173-L175](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/dbt/include/trino/macros/adapters.sql#L173-L175)) | Yes. Trino docs: "The connector supports replacing an existing table, as an atomic operation" ([Replace tables](https://trino.io/docs/current/connector/iceberg.html)) | Kept. Trino docs: the replacement "creates a new snapshot with the new table definition as part of the table history", and earlier snapshots stay queryable with time travel |
| `drop` | Drop, then CTAS | No, same as `iceberg_table` | Restarts |
| `skip` | `CREATE TABLE IF NOT EXISTS` | Doesn't rebuild | |

dbt's Trino docs recommend `replace` when the connector supports `CREATE OR REPLACE` ([Trino configs](https://docs.getdbt.com/reference/resource-configs/trino-configs)). Trino's REST catalog code builds the replacement with Iceberg's `createOrReplaceTransaction()` ([TrinoRestCatalog.java L470-L485](https://github.com/trinodb/trino/blob/6b823e91c6134404577086cb8d7be21efb3869e8/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/catalog/rest/TrinoRestCatalog.java#L470-L485)). Nobody has tested `replace` against Lakekeeper for this page.

**Incremental.** The strategies are `append`, `merge`, `delete+insert` and `microbatch` ([impl.py L363-L364](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/dbt/adapters/trino/impl.py#L363-L364)). A full refresh uses the model's `on_table_exists` ([Trino configs](https://docs.getdbt.com/reference/resource-configs/trino-configs)). The Iceberg connector supports `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` and `MERGE` ([Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)). Under autocommit, the DELETE and INSERT of `delete+insert` are two commits. That's inferred from the connection mode.

**Other notes.**

- dbt-trino 1.10.0 added `catalogs.yml` support. Its catalog integration can set table format, file format and location, but not the metastore, which "cannot be configured using dbt's generated SQL" ([_trino_catalog_metastore.py](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/dbt/adapters/trino/catalogs/_trino_catalog_metastore.py)).
- dbt snapshots use `TIMESTAMP(3) WITH TIME ZONE`, which dbt's docs say the Iceberg connector doesn't support ([Trino configs](https://docs.getdbt.com/reference/resource-configs/trino-configs)). This project has no snapshots.
- dbt-trino's own test stack runs Iceberg against a Hive metastore, not a REST catalog ([iceberg.properties](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/docker/trino/catalog/iceberg.properties)). Lakekeeper documents and ships examples for Trino ([Lakekeeper: Trino](https://docs.lakekeeper.io/docs/latest/engines/#trino)).
- No primary source gives a memory minimum for a single Trino container. The image sizes the JVM heap at 80% of the container's memory ([jvm.config](https://github.com/trinodb/trino/blob/6b823e91c6134404577086cb8d7be21efb3869e8/core/docker/default/etc/jvm.config)).

**Cost to this repo.** One JVM service to add and configure. Models are written for DuckDB, so each one needs checking against Trino's SQL. The CI docs build uses an in-memory DuckDB `lake` ([profiles.yml](../profiles.yml)) and would need another approach, which wasn't investigated.

## Option 4: dbt-spark

| | |
|---|---|
| Latest release | 1.11.0, 2026-07-16 ([PyPI](https://pypi.org/pypi/dbt-spark/json)) |
| dbt-core range | `dbt-core>=1.8.0rc1,<2.0` ([PyPI](https://pypi.org/pypi/dbt-spark/json)) |
| Maintainer | dbt Labs, in the [dbt-adapters](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-spark) monorepo |
| New service | A Spark Thrift Server, or Java and PySpark inside the Dagster image |

**Connecting.** dbt-spark's methods are `thrift`, `http`, `odbc` and `session` ([connections.py L77-L81](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/adapters/spark/connections.py#L77-L81)). Spark Connect is an open feature request ([dbt-adapters#493](https://github.com/dbt-labs/dbt-adapters/issues/493)).

- `thrift` connects to a Spark Thrift Server, which you start with `sbin/start-thriftserver.sh` ([Spark docs](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html)). The Lakekeeper catalog, the Iceberg runtime and the AWS bundle jars are configured on that server.
- `session` runs Spark inside the dbt process. It builds a `SparkSession` with the profile's `server_side_parameters` as Spark config ([session.py L116-L121](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/adapters/spark/session.py#L116-L121)). That means a JVM and PySpark in the Dagster container.

dbt-spark has no database level. It sets `database` to `None` ([connections.py L139](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/adapters/spark/connections.py#L139)), and dbt's docs say never to set it ([Spark configs](https://docs.getdbt.com/reference/resource-configs/spark-configs)). The Lakekeeper catalog therefore has to be Spark's default catalog (`spark.sql.defaultCatalog`), as in Lakekeeper's example ([Spark notebook](https://github.com/lakekeeper/lakekeeper/blob/69ec61f129761ce607027581ad68a0fce5df1898/examples/minimal/notebooks/Spark.ipynb)). Putting the catalog in the schema name instead has an open bug ([dbt-adapters#2129](https://github.com/dbt-labs/dbt-adapters/issues/2129)).

Lakekeeper's docs say Spark "supports credential vending for all storage types, so that no credentials need to be specified" ([Lakekeeper: Spark](https://docs.lakekeeper.io/docs/latest/engines/#spark)). The example sets no S3 endpoint or keys.

**What `table` runs.** With `file_format='iceberg'` and an existing Iceberg table, dbt-spark skips the drop and runs `create or replace table <model> ... as <model SQL>` ([table.sql L14-L29](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/include/spark/macros/materializations/table.sql#L14-L29), [adapters.sql L146-L152](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/include/spark/macros/adapters.sql#L146-L152)). The macro's comment says this is "so we don't have the table unavailable". Iceberg's Spark docs say RTAS is atomic with a `SparkCatalog`, which a REST catalog uses, and that "Atomic table replacement creates a new snapshot with the results of the `SELECT` query, but keeps table history" ([Iceberg Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/#replace-table-as-select)).

dbt-spark recognizes an Iceberg table by `Provider: iceberg` in Spark's table metadata ([impl.py L225](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/adapters/spark/impl.py#L225)). For Iceberg v2 tables it falls back from `SHOW TABLE EXTENDED` to `SHOW TABLES` plus one `DESCRIBE EXTENDED` per table ([L260](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/adapters/spark/impl.py#L260)). If detection fails, the macro drops and recreates the table instead.

**Incremental.** The strategies are `append`, `merge`, `insert_overwrite` and `microbatch`. `merge` needs Delta, Iceberg or Hudi ([validate.sql](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/include/spark/macros/materializations/incremental/validate.sql)). A full refresh of an Iceberg incremental model drops the table first ([dbt-adapters#1145](https://github.com/dbt-labs/dbt-adapters/issues/1145), open).

**Open Iceberg issues.**

- [#485](https://github.com/dbt-labs/dbt-adapters/issues/485): `dbt docs generate` has no column information for Iceberg tables.
- [#467](https://github.com/dbt-labs/dbt-adapters/issues/467): dropping columns on Iceberg.
- [#490](https://github.com/dbt-labs/dbt-adapters/issues/490): Iceberg tables couldn't be read over the `session` method. The current session cursor wraps `AnalysisException` in `DbtRuntimeError` ([session.py L125-L126](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark/src/dbt/adapters/spark/session.py#L125-L126)), which is the fix the issue proposes. The issue is still open, and the fix is not verified.

**Size.** Spark's default driver memory is `1g` ([Spark configuration](https://spark.apache.org/docs/latest/configuration.html)), plus JVM overhead. The total container size is not verified.

**Cost to this repo.** A JVM service, or a JVM in the Dagster image, plus Iceberg jars to manage. Models have to move to Spark SQL. dbt v2 lists Spark as beta, CLI only, over Thrift or Livy ([supported adapters snippet](https://github.com/dbt-labs/docs.getdbt.com/blob/c075c4ea09fcece753f96466dfc487728b4c6eeb/website/snippets/_fusion-dwh-local.md)), which matters only for a later move to v2.

## Option 5: dbt-starrocks

| | |
|---|---|
| Latest release | 1.12.2, 2026-09-15 ([releases](https://github.com/StarRocks/dbt-starrocks/releases)) |
| dbt-core range | `dbt-core>=1.9.0` on Python below 3.14 ([PyPI](https://pypi.org/pypi/dbt-starrocks/json)) |
| Maintainer | StarRocks ([dbt docs](https://docs.getdbt.com/docs/local/connect-data-platform/starrocks-setup)) |
| New service | StarRocks frontend and backend, or the all-in-one image. The quick start asks for 4 GB of RAM for Docker ([quick start](https://docs.starrocks.io/docs/quick_start/shared-nothing/)) |

**Connecting.** StarRocks attaches the REST catalog as an external catalog (`iceberg.catalog.type=rest`). For S3-compatible storage it takes `aws.s3.endpoint` and `aws.s3.enable_path_style_access`. Vended credentials with a REST catalog are supported "from v4.0 onwards" ([StarRocks Iceberg catalog](https://github.com/StarRocks/starrocks/blob/d18a317e5e52470f296c5dc5146c0c21ba27a217/docs/en/data_source/catalog/iceberg/iceberg.md)). Lakekeeper's guide is written for StarRocks 3.3, "which does not support vended-credentials for AWS S3 with custom endpoints" ([Lakekeeper: StarRocks](https://docs.lakekeeper.io/docs/latest/engines/#starrocks)). Its example, on StarRocks 4.0.1, still passes static MinIO keys ([StarRocks notebook](https://github.com/lakekeeper/lakekeeper/blob/69ec61f129761ce607027581ad68a0fce5df1898/examples/minimal/notebooks/Starrocks.ipynb)). A model writes to the catalog when it sets `catalog` and `database` ([README](https://github.com/StarRocks/dbt-starrocks/blob/v1.12.2/README.md#write-to-catalog)).

**What `table` runs.** On an external catalog, `on_table_exists` defaults to `replace`, which drops the table and then creates it ([table.sql L24, L56-L58](https://github.com/StarRocks/dbt-starrocks/blob/v1.12.2/dbt/include/starrocks/macros/materializations/table.sql#L56-L58)). That's the same gap and history reset as `iceberg_table`. The other options are `append` and `ignore`. StarRocks' Iceberg DDL docs list no table rename or `CREATE OR REPLACE` ([DDL](https://github.com/StarRocks/starrocks/blob/d18a317e5e52470f296c5dc5146c0c21ba27a217/docs/en/data_source/catalog/iceberg/DDL.md)).

**Incremental.** On an external catalog, `unique_key` can't deduplicate, so plain `INSERT INTO` appends. `insert_overwrite` and `dynamic_overwrite` replace the partitions the query produced ([README](https://github.com/StarRocks/dbt-starrocks/blob/v1.12.2/README.md#write-to-catalog)). A full refresh drops and recreates the table ([incremental.sql](https://github.com/StarRocks/dbt-starrocks/blob/v1.12.2/dbt/include/starrocks/macros/materializations/models/incremental.sql)). StarRocks supports `INSERT OVERWRITE` into Iceberg from v3.1, `DELETE` from v4.1 and `UPDATE` from v4.2 ([Iceberg DML](https://docs.starrocks.io/docs/data_source/catalog/iceberg/DML/)).

An `incremental` model with `insert_overwrite` on an unpartitioned Iceberg table would overwrite the whole table in place. Whether StarRocks commits that as one Iceberg snapshot is not verified, and it's a workaround, not the `table` materialization.

## Adapters that don't fit

- **dbt-doris** 1.0.0, from 2026-03-17 ([PyPI](https://pypi.org/pypi/dbt-doris/json)). Its macros build Doris internal tables, with `distributed by` clauses and a table swap through `exchange_relation` ([source](https://github.com/apache/doris/tree/310e302bcc975b91265ac589a264d80fb25160e5/extension/dbt-doris)). Its README doesn't mention external or Iceberg catalogs. Lakekeeper's engine page doesn't list Doris ([Lakekeeper engines](https://docs.lakekeeper.io/docs/latest/engines/)).
- **dbt-dremio** 1.11.0, from 2026-09-11. Its `table` materialization drops the old table and then creates the new one ([table.sql L28](https://github.com/dremio/dbt-dremio/blob/v1.11.0/dbt/include/dremio/macros/materializations/table/table.sql#L28)). Dremio's Iceberg REST Catalog source is listed under the Enterprise edition in the 26.x docs ([Dremio docs](https://docs.dremio.com/current/data-sources/lakehouse-catalogs/iceberg-rest-catalog)).
- **A PyIceberg-based adapter or dbt-duckdb plugin.** No published one was found (GitHub search, 2026-10-08). dbt-duckdb's built-in `iceberg` plugin only implements `load`, so it reads sources but doesn't write ([iceberg.py](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/dbt/adapters/duckdb/plugins/iceberg.py)). A plugin can implement `store`, which runs after an `external` materialization writes its file ([README](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/README.md#writing-your-own-plugins)). PyIceberg's `Table.overwrite` runs inside one transaction, producing delete and append snapshots in a single commit ([source](https://github.com/apache/iceberg-python/blob/068aae50402285657b41f5357acb67672b0a053d/pyiceberg/table/__init__.py#L1744-L1780)), and PyIceberg asks for vended credentials by default ([configuration](https://py.iceberg.apache.org/configuration/)). A custom plugin could use it to replace data atomically while keeping history, but it would be new code that nobody has tested here.
- **dbt-risingwave** 1.12.0 ([PyPI](https://pypi.org/pypi/dbt-risingwave/json)). RisingWave is a streaming database. Lakekeeper lists it, but dbt-risingwave's docs mention Iceberg only for sinks ([zero-downtime-rebuilds.md](https://github.com/risingwavelabs/dbt-risingwave/blob/main/docs/zero-downtime-rebuilds.md)). It wasn't evaluated further.
- **Cloud adapters.** These need a cloud account, and the warehouse would have to reach a catalog running on a laptop, so they don't fit a local, vendor-agnostic demo.
  - dbt-athena writes Iceberg through the AWS Glue Data Catalog or Amazon S3 Tables ([Athena configs](https://docs.getdbt.com/reference/resource-configs/athena-configs)). dbt-glue runs on AWS Glue sessions ([PyPI](https://pypi.org/pypi/dbt-glue/json)).
  - Snowflake can write to external Iceberg REST catalogs through catalog-linked databases, "in theory" any catalog that implements the REST API ([dbt: Snowflake Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/snowflake-iceberg-support)).
  - Databricks has "only limited support for reading from external Iceberg catalogs" ([dbt: Databricks Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/databricks-iceberg-support)).
  - BigQuery "today doesn't support connecting to external Iceberg catalogs" ([dbt: BigQuery Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/bigquery-iceberg-support)).

## Option 6: dbt v2

dbt v2.0.0 was tagged on 2026-09-14 in [dbt-labs/dbt-core](https://github.com/dbt-labs/dbt-core/releases), now a Rust codebase. The separate dbt-fusion repo is marked "ARCHIVE" ([dbt-fusion](https://github.com/dbt-labs/dbt-fusion)). dbt announced general availability on 2026-09-16 ([blog](https://docs.getdbt.com/blog/dbt-v2-is-ga)). On PyPI, `dbt` is at 2.0.6 and the Apache-licensed `dbt-oss` at 2.0.5 ([dbt](https://pypi.org/pypi/dbt/json), [dbt-oss](https://pypi.org/pypi/dbt-oss/json)). The upgrade guide installs v2 with `python -m pip install dbt` ([upgrading to v2](https://docs.getdbt.com/docs/dbt-versions/dbt-upgrade/upgrading-to-v2)). DuckDB is listed for local CLI use with no beta label ([supported adapters snippet](https://github.com/dbt-labs/docs.getdbt.com/blob/c075c4ea09fcece753f96466dfc487728b4c6eeb/website/snippets/_fusion-dwh-local.md)).

### What v2's `table` runs on an `iceberg_rest` catalog

dbt's docs list `table` and `incremental` for DuckDB Iceberg but don't say how they write ([DuckDB and Apache Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/duckdb-iceberg-support)). The v2 source does. Everything below is read from the source at tag `v2.0.5`, not tested.

- For DuckDB, a catalog whose `table_format` is `iceberg`, and which isn't DuckLake, gets write strategy `direct_create`, or `direct_create_as_select` when `stage_create_tables: true` is set ([catalog_relation.rs](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-adapter/src/catalog_relation.rs#L1543-L1572)). The enum's comment says the table materialization "skips the temp-table + rename dance entirely, since Iceberg REST attachments do not support `ALTER ... RENAME`" ([L21-L44](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-adapter/src/catalog_relation.rs#L21-L44)).
- With either strategy, the `table` materialization calls `adapter.drop_relation(target_relation)` before anything else ([table.sql L22-L24](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/materializations/table.sql#L22-L24)). On Iceberg that's `drop table if exists <model>`, without `CASCADE`, run with `auto_begin=False` ([adapters.sql L227-L238](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql#L227-L238)). A comment in the same file says "DuckDB autocommits each statement" ([L245-L248](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql#L245-L248)), so the drop commits on its own. That's inferred from the comment and `auto_begin=False`.
- It then builds the model straight into the final name. Under `direct_create` that's an empty `create table <model> (<columns>)` followed by `insert into <model> (...) (<model SQL>)`. Under `direct_create_as_select` it's `create table <model> as (...)` ([adapters.sql L61-L123](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql#L61-L123)). Then come hooks, grants, `persist_docs` and `COMMIT`.

That's the same drop, commit, create, commit as `iceberg_table`. While the model builds, the table doesn't exist. A failed build leaves no table, and history restarts on every run. v2 does run grants and `persist_docs`, which `iceberg_table` skips. Python models that target an Iceberg catalog stop with a compile error: "DuckDB Python models cannot materialize to an Iceberg catalog that requires create-then-insert writes" ([adapters.sql L124-L127](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/adapters.sql#L124-L127)).

**Incremental in v2.** On an existing table, the incremental materialization builds a temporary table and runs the strategy SQL in one batch, then commits ([incremental.sql](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/materializations/incremental.sql)). The strategies are `append`, `delete+insert`, `merge` and `microbatch` ([strategy macros](https://github.com/dbt-labs/dbt-core/tree/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros/materializations/incremental_strategy)). These change the table in place, so history is kept. A full refresh builds an intermediate table and renames it into place. Whether that works against Lakekeeper, given the rename limits above, is not verified.

### dagster-dbt and dbt v2

- Dagster's dbt Fusion support is in preview ([Dagster & dbt Fusion](https://docs.dagster.io/integrations/libraries/dbt/dbt-fusion)). Support for dbt v2 is requested in [dagster#34233](https://github.com/dagster-io/dagster/issues/34233), which was opened on 2026-09-26 and has no comments.
- `DbtCliResource` picks the `dbt_executable` argument, then a `dbtf` on `PATH`, then `dbt` ([Dagster docs](https://docs.dagster.io/integrations/libraries/dbt/dbt-fusion#how-dagster-selects-a-dbt-executable), [resource.py L40-L47](https://github.com/dagster-io/dagster/blob/2ecaed2ba3f408f2e3692224f10c025e2a36d6fc/python_modules/libraries/dagster-dbt/dagster_dbt/core/resource.py#L40-L47)). `DbtProject.prepare_if_dev()` creates `DbtCliResource(project_dir=project)` with no executable argument ([dbt_project.py L125, L134](https://github.com/dagster-io/dagster/blob/2ecaed2ba3f408f2e3692224f10c025e2a36d6fc/python_modules/libraries/dagster-dbt/dagster_dbt/dbt_project.py#L125)). So it uses the same default, which picks `dbtf` when it's on `PATH`. A `dbtf` symlink in the image would switch both [transform.py](../../orchestrate/assets/transform.py) and [definitions.py](../../orchestrate/definitions.py) to v2 without code changes. That's inferred from the source, not tested.
- dagster-dbt still depends on dbt-core, whose `dbt` entrypoint can shadow the v2 binary ([dagster#33513](https://github.com/dagster-io/dagster/issues/33513), open). Dagster's docs advise against uninstalling dbt-core and suggest the `dbtf` symlink instead.
- Column metadata, column lineage and row counts aren't supported on Fusion ([dagster#34227](https://github.com/dagster-io/dagster/issues/34227)). This project doesn't use them.

**For this repo,** moving to v2 means removing dbt-duckdb, installing the v2 binary and linking it as `dbtf`, moving the `ATTACH` into `catalogs.yml`, and setting `use_catalogs_v2` and `catalog_name`. Models could then use the built-in `table`, but against Lakekeeper it does what `iceberg_table` does now. The gains are grants, `persist_docs` and built-in incremental models, not an atomic rebuild.

## Comparison

| Option | dbt v1 (1.12) | dagster-dbt unchanged | Atomic rebuild | Keeps history | Incremental on Iceberg | New infra | Maturity |
|---|---|---|---|---|---|---|---|
| Current `iceberg_table` | Yes | Yes | No | No | No | None | In use, tested on 2026-10-07 |
| 1. dbt-duckdb, DELETE + INSERT in one transaction | Yes | Yes | Likely, if DuckDB sends one commit. Not atomic when columns change. Untested | Yes | Not built. DuckDB supports `MERGE INTO`, `DELETE`, `UPDATE` | None | Custom macro, untested. Needs outside compaction and snapshot expiry |
| 2. dbt-duckdb, commit before rename (PR #747) | Yes | Yes | Nearly. Table missing only between the last two commits. Failed build keeps old table. Untested | No | dbt-duckdb's incremental, untested on Iceberg | None | Unmerged PR, two macro overrides |
| 3. dbt-trino, `on_table_exists='replace'` | Yes (`<2.0`) | Yes, adapter swap only | Yes, per Trino docs | Yes | `append`, `merge`, `delete+insert`, `microbatch` | Trino container | Starburst, regular releases |
| 3. dbt-trino, default `rename` | Yes | Yes | Nearly. Gap between two autocommitted renames | No | As above | Trino container | As above |
| 4. dbt-spark, `file_format='iceberg'` | Yes (`<2.0`) | Yes, adapter swap only | Yes, RTAS with `SparkCatalog` | Yes | `append`, `merge`, `insert_overwrite`, `microbatch`. Full refresh drops the table | Spark Thrift Server, or JVM in the Dagster image | dbt Labs. Several open Iceberg bugs |
| 5. dbt-starrocks, external catalog | Yes | Yes, adapter swap only | No, drop then create | No | Partition overwrite, append. No `unique_key` | StarRocks, about 4 GB | StarRocks. Lakekeeper example still uses static keys |
| 6. dbt v2, DuckDB + `catalogs.yml` | No | No. Fusion support in preview, `dbtf` symlink needed | No, drop then create | No (`table`). Yes (incremental) | `append`, `delete+insert`, `merge`, `microbatch`. Full refresh untested | None | dbt v2 GA in September 2026. Dagster support in preview |

dagster-dbt runs the dbt CLI ([Dagster docs](https://docs.dagster.io/integrations/libraries/dbt/dbt-fusion)), so "adapter swap only" means replacing `dbt-duckdb` in [pyproject.toml](../pyproject.toml) and the profile. No Dagster code changes.

## Trade-offs

Of the adapters that run on dbt-core 1.12, only dbt-trino with `replace` and dbt-spark with `file_format='iceberg'` issue a statement that the engine documents as an atomic replace that keeps snapshot history. Both bring a JVM service into a stack that has none today, plus a SQL dialect change and new configuration to maintain. Trino's case rests on its own docs and its REST catalog code. Spark's rests on Iceberg's docs and dbt-spark's macros, and dbt-spark has more open Iceberg bugs and no database level.

The dbt-duckdb options keep the stack as it is. Option 2 reuses statements already tested here and brings back the built-in materialization's features, but history still restarts and the fix lives in overrides of adapter internals. Option 1 keeps history and could make rebuilds atomic, but it's the least proven: it relies on untested commit behavior, it accumulates delete files and snapshots, and open-source Lakekeeper won't clean them up.

Waiting doesn't fix this on its own timeline. DuckDB's `main` branch still rejects all four statements. dbt-duckdb's fix is an unmerged PR. dbt v2's DuckDB adapter does the same drop and create as the current macro, and dagster-dbt's support for it is in preview.

## Not verified

- Whether DuckDB 1.5.6 commits a `DELETE` and an `INSERT` on the same Iceberg table in one transaction as a single Lakekeeper commit (Option 1). Also how many snapshots that creates, and whether tables DuckDB creates set `write.delete.mode`.
- Whether the PR #747 overrides work end to end against Lakekeeper, including `persist_docs` after a rename in the same transaction (Option 2).
- Whether Trino's `CREATE OR REPLACE` and Spark's RTAS succeed against Lakekeeper v0.13.6 specifically. Both are documented as atomic for Iceberg REST catalogs in general.
- Whether dbt-trino's CI covers dbt-core 1.12. Its dependency range allows it.
- Whether dbt-spark detects Lakekeeper tables as Iceberg (`Provider: iceberg`), and whether issue #490 is fixed.
- Whether StarRocks 4.x vended credentials work with MinIO and Lakekeeper, and whether its `INSERT OVERWRITE` on Iceberg commits as one snapshot.
- Memory needs for Trino and Spark containers. No primary source gives a minimum.
- Whether dbt v2's incremental full refresh works against Lakekeeper, and which DuckDB build v2 downloads.
- Whether DuckDB 2.0.0 ships from the extension's current `main` branch.
- Whether any unpublished or private PyIceberg-based dbt adapter exists. The search covered GitHub and PyPI only.

## Sources

dbt-duckdb and DuckDB

- dbt-duckdb [releases](https://github.com/duckdb/dbt-duckdb/releases), [1.11.0 adapters.sql](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/dbt/include/duckdb/macros/adapters.sql), [1.11.0 table.sql](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/dbt/include/duckdb/macros/materializations/table.sql), [iceberg plugin](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/dbt/adapters/duckdb/plugins/iceberg.py), [README](https://github.com/duckdb/dbt-duckdb/blob/1.11.0/README.md)
- dbt-duckdb PRs [#747](https://github.com/duckdb/dbt-duckdb/pull/747), [#725](https://github.com/duckdb/dbt-duckdb/pull/725), [#833](https://github.com/duckdb/dbt-duckdb/pull/833), [#755](https://github.com/duckdb/dbt-duckdb/pull/755)
- DuckDB [Writing to Iceberg](https://duckdb.org/docs/current/core_extensions/iceberg/writing), [Iceberg REST catalogs](https://duckdb.org/docs/current/core_extensions/iceberg/iceberg_rest_catalogs), [release calendar](https://duckdb.org/release_calendar), [releases](https://github.com/duckdb/duckdb/releases)
- duckdb-iceberg source on [v1.5-variegata](https://github.com/duckdb/duckdb-iceberg/tree/5dcf5070c50341c2e4b4403b67ed4cbc7afaa37b) and [main](https://github.com/duckdb/duckdb-iceberg/tree/25509bdb99623d1894d139eb3bc5199d040ea4b2)
- duckdb-iceberg [#924](https://github.com/duckdb/duckdb-iceberg/pull/924), [#784](https://github.com/duckdb/duckdb-iceberg/issues/784), [#620](https://github.com/duckdb/duckdb-iceberg/issues/620), [#1000](https://github.com/duckdb/duckdb-iceberg/issues/1000), [#1178](https://github.com/duckdb/duckdb-iceberg/pull/1178), [#1287](https://github.com/duckdb/duckdb-iceberg/pull/1287), [#1260](https://github.com/duckdb/duckdb-iceberg/issues/1260)

dbt

- [DuckDB and Apache Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/duckdb-iceberg-support), [catalogs.yml](https://docs.getdbt.com/docs/build/iceberg/catalogs-yml), [upgrading to v2](https://docs.getdbt.com/docs/dbt-versions/dbt-upgrade/upgrading-to-v2), [v2 GA post](https://docs.getdbt.com/blog/dbt-v2-is-ga), [dispatch config](https://docs.getdbt.com/reference/project-configs/dispatch-config)
- dbt v2 source at [v2.0.5](https://github.com/dbt-labs/dbt-core/tree/v2.0.5): [catalog_relation.rs](https://github.com/dbt-labs/dbt-core/blob/v2.0.5/crates/dbt-adapter/src/catalog_relation.rs), [DuckDB macros](https://github.com/dbt-labs/dbt-core/tree/v2.0.5/crates/dbt-loader/src/dbt_macro_assets/dbt-duckdb/macros); [dbt-core releases](https://github.com/dbt-labs/dbt-core/releases); [dbt-fusion (archived)](https://github.com/dbt-labs/dbt-fusion)
- dbt-adapters [drop.sql](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-adapters/src/dbt/include/global_project/macros/relations/drop.sql), [rename.sql](https://github.com/dbt-labs/dbt-adapters/blob/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-adapters/src/dbt/include/global_project/macros/relations/rename.sql)
- Supported v2 adapters: [_fusion-dwh-local.md](https://github.com/dbt-labs/docs.getdbt.com/blob/c075c4ea09fcece753f96466dfc487728b4c6eeb/website/snippets/_fusion-dwh-local.md)
- PyPI: [dbt](https://pypi.org/pypi/dbt/json), [dbt-oss](https://pypi.org/pypi/dbt-oss/json), [dbt-duckdb](https://pypi.org/pypi/dbt-duckdb/json), [dbt-trino](https://pypi.org/pypi/dbt-trino/json), [dbt-spark](https://pypi.org/pypi/dbt-spark/json), [dbt-starrocks](https://pypi.org/pypi/dbt-starrocks/json), [dbt-doris](https://pypi.org/pypi/dbt-doris/json), [dbt-dremio](https://pypi.org/pypi/dbt-dremio/json), [dbt-risingwave](https://pypi.org/pypi/dbt-risingwave/json), [dbt-glue](https://pypi.org/pypi/dbt-glue/json), [dagster-dbt](https://pypi.org/pypi/dagster-dbt/0.29.25/json)

Trino and dbt-trino

- [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html), [metastores (REST catalog)](https://trino.io/docs/current/object-storage/metastores.html), [releases](https://github.com/trinodb/trino/releases), [TrinoRestCatalog.java](https://github.com/trinodb/trino/blob/6b823e91c6134404577086cb8d7be21efb3869e8/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/catalog/rest/TrinoRestCatalog.java), [docker jvm.config](https://github.com/trinodb/trino/blob/6b823e91c6134404577086cb8d7be21efb3869e8/core/docker/default/etc/jvm.config)
- dbt-trino [v1.10.6 source](https://github.com/starburstdata/dbt-trino/tree/v1.10.6), [CHANGELOG](https://github.com/starburstdata/dbt-trino/blob/v1.10.6/CHANGELOG.md), [Trino configs](https://docs.getdbt.com/reference/resource-configs/trino-configs), [Trino setup](https://docs.getdbt.com/docs/local/connect-data-platform/trino-setup)

Spark and dbt-spark

- dbt-spark [source](https://github.com/dbt-labs/dbt-adapters/tree/3d853e386271786909c0dff001a9634e7fbccf8e/dbt-spark), [Spark configs](https://docs.getdbt.com/reference/resource-configs/spark-configs), issues [#493](https://github.com/dbt-labs/dbt-adapters/issues/493), [#2129](https://github.com/dbt-labs/dbt-adapters/issues/2129), [#1145](https://github.com/dbt-labs/dbt-adapters/issues/1145), [#485](https://github.com/dbt-labs/dbt-adapters/issues/485), [#467](https://github.com/dbt-labs/dbt-adapters/issues/467), [#490](https://github.com/dbt-labs/dbt-adapters/issues/490)
- [Iceberg Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/#replace-table-as-select), [Iceberg spec](https://iceberg.apache.org/spec/), [Spark configuration](https://spark.apache.org/docs/latest/configuration.html), [Spark Thrift server](https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html)

StarRocks, Doris, Dremio, PyIceberg, RisingWave

- dbt-starrocks [v1.12.2 source and README](https://github.com/StarRocks/dbt-starrocks/tree/v1.12.2), StarRocks [Iceberg catalog](https://github.com/StarRocks/starrocks/blob/d18a317e5e52470f296c5dc5146c0c21ba27a217/docs/en/data_source/catalog/iceberg/iceberg.md), [Iceberg DML](https://docs.starrocks.io/docs/data_source/catalog/iceberg/DML/), [Iceberg DDL](https://github.com/StarRocks/starrocks/blob/d18a317e5e52470f296c5dc5146c0c21ba27a217/docs/en/data_source/catalog/iceberg/DDL.md), [quick start](https://docs.starrocks.io/docs/quick_start/shared-nothing/)
- [dbt-doris source](https://github.com/apache/doris/tree/310e302bcc975b91265ac589a264d80fb25160e5/extension/dbt-doris), [dbt-dremio v1.11.0](https://github.com/dremio/dbt-dremio/tree/v1.11.0), [Dremio Iceberg REST Catalog source](https://docs.dremio.com/current/data-sources/lakehouse-catalogs/iceberg-rest-catalog)
- PyIceberg [API](https://py.iceberg.apache.org/api/), [configuration](https://py.iceberg.apache.org/configuration/), [table/\_\_init\_\_.py](https://github.com/apache/iceberg-python/blob/068aae50402285657b41f5357acb67672b0a053d/pyiceberg/table/__init__.py)
- [dbt-risingwave](https://github.com/risingwavelabs/dbt-risingwave)

Cloud adapters

- [Athena configs](https://docs.getdbt.com/reference/resource-configs/athena-configs), [Snowflake Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/snowflake-iceberg-support), [Databricks Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/databricks-iceberg-support), [BigQuery Iceberg](https://docs.getdbt.com/docs/build/iceberg/adapters/bigquery-iceberg-support)

Lakekeeper

- [Query engines](https://docs.lakekeeper.io/docs/latest/engines/) ([source](https://github.com/lakekeeper/lakekeeper/blob/69ec61f129761ce607027581ad68a0fce5df1898/docs/docs/engines.md)), [concepts](https://docs.lakekeeper.io/docs/latest/concepts/), [table maintenance](https://docs.lakekeeper.io/docs/latest/table-maintenance/), [minimal example](https://github.com/lakekeeper/lakekeeper/tree/69ec61f129761ce607027581ad68a0fce5df1898/examples/minimal), [engine tracking issue #399](https://github.com/lakekeeper/lakekeeper/issues/399), [releases](https://github.com/lakekeeper/lakekeeper/releases)

Dagster

- [Dagster & dbt Fusion](https://docs.dagster.io/integrations/libraries/dbt/dbt-fusion), [CHANGES.md](https://github.com/dagster-io/dagster/blob/2ecaed2ba3f408f2e3692224f10c025e2a36d6fc/CHANGES.md), [resource.py](https://github.com/dagster-io/dagster/blob/2ecaed2ba3f408f2e3692224f10c025e2a36d6fc/python_modules/libraries/dagster-dbt/dagster_dbt/core/resource.py), [dbt_project.py](https://github.com/dagster-io/dagster/blob/2ecaed2ba3f408f2e3692224f10c025e2a36d6fc/python_modules/libraries/dagster-dbt/dagster_dbt/dbt_project.py), issues [#34233](https://github.com/dagster-io/dagster/issues/34233), [#33513](https://github.com/dagster-io/dagster/issues/33513), [#34227](https://github.com/dagster-io/dagster/issues/34227)
