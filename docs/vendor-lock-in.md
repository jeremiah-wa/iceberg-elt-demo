# Can Iceberg get you out of vendor lock-in?

This demo started with one question: if every table is Apache Iceberg, can you change the engine, the catalog or the vendor without being stuck? This page answers it. It uses what the project tested, and the [Iceberg Roulette](https://b-per.github.io/iceberg-roulette/#matrix) compatibility matrix for the engines the project didn't test. It's for anyone deciding whether to build on Iceberg to stay portable. You should know what a query engine and a catalog are.

## The short answer

Partly. Iceberg makes the data portable. When this project moved its dbt models from DuckDB to Trino, the tables and their snapshot history came along without an export or a migration. The SQL, the way each engine writes tables, and table maintenance didn't come along. Each of those had to be redone for the new engine.

So Iceberg turns lock-in into a switching cost. The cost is lower than moving a warehouse, but it isn't zero. Which catalog you pick decides a lot of it.

## What the project tried

Every table is Iceberg, stored as Parquet files in MinIO and registered in Lakekeeper, an open-source Iceberg REST catalog. Three engines work on the same tables:

| Engine | What it does |
|---|---|
| dlt, through pyiceberg | Writes `raw.issues` and `raw.pull_requests` |
| Trino 483, through dbt-trino | Reads `raw` and writes the `staging` and `marts` models. DuckDB did this until 2026-10-08 |
| DuckDB 1.5.6 | Wrote the models until 2026-10-08. Read the tables while Trino rebuilt them in testing |

Everything runs on a laptop with open-source software. The project never connected a cloud warehouse, so anything this page says about Snowflake, BigQuery, Databricks or AWS comes from Iceberg Roulette and vendor docs, not from a test here.

## What moved without any work

**The tables.** On 2026-10-08 the models moved from DuckDB to Trino. Trino's first run replaced the tables DuckDB had built, and those tables kept their earlier snapshots, so time travel still reaches the DuckDB builds. Nothing was exported or copied ([iceberg-materialization.md](../transform/docs/iceberg-materialization.md#how-this-was-tested)).

**Reads across engines.** While Trino rebuilt `marts.fct_pull_requests`, a DuckDB connection in another container counted its rows every 20 ms. No query failed, and DuckDB switched to the new row count at the moment Trino committed.

**Storage access.** No engine holds the storage keys. Lakekeeper hands pyiceberg and Trino the same kind of short-lived, per-table credentials, so adding an engine doesn't mean giving it the bucket's root keys ([architecture.md](../transform/docs/architecture.md#who-holds-which-keys)).

## What didn't move

### How tables get written

The same table in the same catalog behaved very differently depending on the engine that wrote it.

DuckDB's Iceberg extension rejects `CREATE OR REPLACE TABLE`, `DROP TABLE ... CASCADE`, and a rename after a create in the same transaction. dbt-duckdb's `table` materialization needs all of those, so the project wrote its own materialization that dropped each table, committed and created it again. While a model rebuilt, its table didn't exist. A failed build left no table, and history restarted on every run.

Trino runs the same rebuild as one `CREATE OR REPLACE TABLE ... AS` and commits it as one snapshot. Readers never see a missing table, a failed build keeps the old one, and history is kept. [iceberg-materialization.md](../transform/docs/iceberg-materialization.md#why-not-dbt-duckdb) has the statement-by-statement test results.

Iceberg defines what a table looks like on disk and how a commit works. It doesn't make every engine support every write.

### The SQL

The models were written in DuckDB's SQL and had to change for Trino:

- Trino's `||` only joins strings, so the IDs now cast `number` to `varchar`.
- Trino's `date_diff('hour', ...)` counts whole hours, where DuckDB's counted hour boundaries. `hours_to_close` in `fct_pull_requests` can now be one lower than it was.

The first change fails loudly. The second doesn't. The model runs and the tests pass, but the numbers change. Moving engines means checking results, not only checking that the SQL runs.

### dbt doesn't hide the engine

dbt sends SQL to the engine. It doesn't write Iceberg itself. Each adapter has its own `table` materialization, and those materializations decided whether rebuilds were atomic:

| Adapter | What `table` does on Iceberg |
|---|---|
| dbt-duckdb 1.11 | Fails. DuckDB rejects the renames and `CASCADE` it sends |
| dbt-trino 1.10.6, `on_table_exists: replace` | One atomic replace that keeps history |
| dbt v2 on DuckDB | Drops the table, commits, creates it again. Same gap as the old workaround |

dbt-trino doesn't run Python models at all. [iceberg-adapter-options.md](../transform/docs/iceberg-adapter-options.md) compares more adapters.

### Table maintenance

Every rebuild adds a snapshot, and every snapshot keeps its files until someone expires it. In this stack only Trino does that, with `alter table ... execute expire_snapshots(...)`. Open-source Lakekeeper doesn't expire snapshots; the endpoints for it appear only in the API spec for Lakekeeper Plus, the commercial edition. DuckDB 1.5 has no compaction function. Whichever engine runs maintenance is a dependency, even if it isn't the one you query with.

## What Iceberg Roulette adds

[Iceberg Roulette](https://b-per.github.io/iceberg-roulette/#matrix) rates whether one engine can write an Iceberg table that another engine can read, through each catalog. For every engine and catalog it records write support and read support, as full, partial or none, with the limitations and links to sources. A writer and reader pair through one catalog gets the lower of the writer's write rating and the reader's read rating. A few pairs that someone tested end to end override that rule.

The matrix below shows the best rating over all catalogs for each pair, so the catalog behind each cell can differ. Rows are writers and columns are readers. F is full, P is partial and `-` is none. The data is as published on 2026-10-08.

| Writer ↓ Reader → | Snowflake | BigQuery | Databricks | DuckDB | Redshift | Trino | Athena | PostgreSQL | Cloudflare |
|---|---|---|---|---|---|---|---|---|---|
| Snowflake | F | P | F | F | F | F | F | P | - |
| BigQuery | P | P | P | P | P | P | P | P | - |
| Databricks | F | P | F | F | P | F | P | P | - |
| DuckDB | P | - | P | P | P | P | P | P | P |
| Redshift | F | - | P | P | F | F | F | P | - |
| Trino | F | P | F | F | F | F | F | P | P |
| Athena | F | - | P | P | F | F | F | P | - |
| PostgreSQL | - | - | P | - | - | P | - | P | - |
| Cloudflare | - | - | - | - | - | - | - | - | - |

Write support through a REST catalog, which is what Lakekeeper is:

| Engine | Writes through REST | Notes from Iceberg Roulette |
|---|---|---|
| Snowflake | Full | Quoting and case sensitivity can be inconsistent across REST integrations |
| Trino | Full | No limitations listed |
| DuckDB | Partial | INSERT, UPDATE, DELETE and MERGE work. UPDATE and DELETE are blocked on tables with a sort order up to DuckDB 1.5.6 |
| BigQuery | Partial | Only to Google's own managed REST endpoint, "not a self-hosted open catalog" |
| Databricks | Partial | Needs Spark cluster settings. "Not an officially documented Databricks workflow", since Unity Catalog is the recommended catalog |
| Redshift, Athena | None | They write Iceberg only through AWS Glue and S3 Tables |

What the matrix shows:

- **Reads are broad, writes are narrow.** Seven of the nine engines can read through more catalogs than they can write to, and none can write to more than it reads. Redshift and Athena write only to AWS catalogs, and BigQuery writes through REST only to Google's.
- **The vendor's catalog decides who else can write.** Google warns against running BigQuery DDL or DML on a table once other engines write to it. Databricks points you to Unity Catalog. If you put your tables in a vendor's catalog, other engines can usually read them, but the vendor still controls the writes.
- **Trino and Snowflake are the most interoperable writers.** Each has full support with six of the nine readers. This project's writer, Trino through a REST catalog, is in a good spot.
- **DuckDB reads everywhere and writes with limits.** It has full read support through REST, but no better than partial as a writer. DuckDB to BigQuery doesn't work at all, because BigQuery's Avro reader can't read the manifests DuckDB writes. Iceberg Roulette traces that to BigQuery, not DuckDB.
- **Some engines are readers only, or nearly.** Cloudflare's Basin SQL can't write Iceberg at all. PostgreSQL writes only through pg_lake, and only Databricks and Trino can read those tables, both with limits.

The matrix agrees with what this project found. Having Iceberg everywhere doesn't make engines interchangeable. Which engine writes, and to which catalog, matters more than the table format.

## Conclusion

Iceberg removes the hardest part of lock-in, which is moving the data. In this project, changing the engine cost:

- rewriting the SQL and checking that the numbers still matched;
- finding out how the new engine writes tables, and choosing the dbt materialization to match;
- moving table maintenance to an engine that can do it.

It didn't cost a data migration, a backfill or the table history.

How much lock-in remains depends on the catalog more than on anything else. An open REST catalog, like Lakekeeper here, lets any engine with good REST support write. A vendor's own catalog lets other engines read but keeps writes with the vendor.

## Keeping the switching cost low

- Use an open REST catalog that you run, or that you can move away from. The catalog decides which engines can write.
- Keep the storage bucket in your own account, so the files don't depend on any engine.
- Write with an engine that has full write support for your catalog in the matrix, and let other engines read.
- Keep the SQL close to standard SQL, and compare results when you change engines. A model that still runs can still return different numbers, like `hours_to_close` here.
- Decide which engine runs snapshot expiry and compaction, and treat it as part of the stack.

## Limits of this answer

- Only open-source engines ran here. Everything about cloud vendors comes from Iceberg Roulette and their docs.
- The data is small: 100 issues and pull requests per repo. Performance and cost at scale weren't tested.
- Engines took turns writing: dlt writes `raw`, and Trino writes the models. Two engines never wrote to the same table at the same time.
- Iceberg Roulette is maintained by its author, and ratings change with every engine release. Wrong entries can be reported as [data-accuracy issues](https://github.com/b-per/iceberg-roulette/issues). The figures here are from 2026-10-08.

## Sources

- [Iceberg Roulette](https://b-per.github.io/iceberg-roulette/#matrix) ([source](https://github.com/b-per/iceberg-roulette)), read on 2026-10-08
- This repo: [architecture.md](../transform/docs/architecture.md), [iceberg-materialization.md](../transform/docs/iceberg-materialization.md), [iceberg-adapter-options.md](../transform/docs/iceberg-adapter-options.md), which link the primary sources for the DuckDB, dbt, Trino and Lakekeeper statements
