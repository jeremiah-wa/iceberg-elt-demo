# transform

A dbt project that reads the raw GitHub tables and builds staging and mart tables. Every model is an Iceberg table in the Lakekeeper catalog. It uses dbt-core 1.12 with the dbt-duckdb adapter, so DuckDB does the compute.

## Models

| Model | Contents | Tests |
|---|---|---|
| `staging.stg_github__issues` | One row per issue. Renamed columns, upper-case `state`, `issue_id` as `repo#number` | `issue_id` unique and not null, `state` in OPEN/CLOSED |
| `staging.stg_github__pull_requests` | One row per PR. `state` is MERGED when `merged_at` is set, otherwise OPEN or CLOSED | `pull_request_id` unique and not null, `state` in OPEN/CLOSED/MERGED |
| `marts.fct_pull_requests` | One row per PR with merge status and `hours_to_close` (null while open) | `pull_request_id` unique and not null |

The sources are `raw.issues` and `raw.pull_requests` from [extract_load](../extract_load/README.md), declared in [models/staging/_sources.yml](models/staging/_sources.yml).

## Run it

Dagster runs `dbt build` as part of the `elt` job and shows the tests as asset checks. To run dbt by hand, use the Dagster container (from `infra/`):

```bash
docker compose exec -w /app/transform dagster dbt build
docker compose exec -w /app/transform dagster dbt build --select marts
```

To query the results, attach the catalog in DuckDB:

```bash
docker compose exec -T dagster python - <<'EOF'
import duckdb
con = duckdb.connect()
con.sql("ATTACH 'demo' AS lake (TYPE iceberg, ENDPOINT 'http://lakekeeper:8181/catalog', AUTHORIZATION_TYPE 'none')")
print(con.sql("select repo, count(*) as prs, count_if(is_merged) as merged from lake.marts.fct_pull_requests group by repo order by repo"))
EOF
```

DuckDB needs no S3 keys here. Lakekeeper hands out short-lived credentials for each table it serves.

## How it writes Iceberg

[docs/architecture.md](docs/architecture.md) explains how dbt, DuckDB, Lakekeeper and MinIO work together, with diagrams of a model build, where the files are stored and who holds which keys. In short:

- [profiles.yml](profiles.yml) loads DuckDB's `iceberg` extension and attaches the Lakekeeper warehouse `demo` as a database called `lake`. `data/transform.duckdb` is DuckDB's local session file. It holds no tables.
- [dbt_project.yml](dbt_project.yml) puts every model in the `lake` database, in the `staging` or `marts` schema. The [generate_schema_name](macros/generate_schema_name.sql) macro keeps those names as they are, instead of dbt's default `<target_schema>_staging`.
- Models use dbt's built-in `table` materialization. It builds each model as `<model>__dbt_tmp`, renames the old table to `<model>__dbt_backup`, renames the new one into place and drops the backup. DuckDB rejects two of the statements dbt-duckdb generates for this on Iceberg, so [macros/iceberg_relations.sql](macros/iceberg_relations.sql) overrides its drop and rename macros. The overrides are a port of [dbt-duckdb#747](https://github.com/duckdb/dbt-duckdb/pull/747), which isn't merged.

> [!NOTE]
> Rebuilds are close to atomic, but not quite. While a model's SQL runs, readers see the old table, and if the SQL fails the old table stays. Between the two renames the table is missing for a moment, under 0.1 s in testing. A failure after the first rename, such as a failing post-hook, leaves the table missing until the next successful run. Each rebuild also starts the table's Iceberg snapshot history over. Don't enable `persist_docs`. DuckDB rejects comments on Iceberg tables, and the error leaves the table missing. [docs/iceberg-materialization.md](docs/iceberg-materialization.md) has the tested details. [docs/iceberg-adapter-options.md](docs/iceberg-adapter-options.md) compares ways to rebuild with no gap and keep history, including other dbt adapters.

## Why dbt-core and not dbt v2

dbt v2 has been generally available since September 2026. It ships its own DuckDB adapter and `catalogs.yml` support for Iceberg REST catalogs, but switching would make rebuilds worse. Against an Iceberg REST catalog, v2's DuckDB `table` materialization drops the table, commits, and creates it again, so the table would be missing for the whole build instead of for a moment. It would add built-in incremental models. [docs/iceberg-materialization.md](docs/iceberg-materialization.md#why-not-dbts-own-iceberg-support) links the v2 source.

dagster-dbt isn't ready for v2 either. Version 0.29.25 requires `dbt-core<1.13`, its dbt Fusion support is in preview ([Dagster docs](https://docs.dagster.io/integrations/libraries/dbt/dbt-fusion)), and dbt v2 support is an open request ([dagster#34233](https://github.com/dagster-io/dagster/issues/34233)). dbt-core and v2 both provide a `dbt` command, and dbt-core's can shadow the v2 binary ([dagster#33513](https://github.com/dagster-io/dagster/issues/33513)). Dagster's docs suggest linking the v2 binary as `dbtf`, which `DbtCliResource` picks ahead of `dbt` when it's on `PATH`. dbt-core 1.12 is the newest version dagster-dbt accepts.

## Docs site

[.github/workflows/dbt-docs.yml](../.github/workflows/dbt-docs.yml) runs `dbt docs generate` on every push to `main` that touches `transform/` and publishes the result to GitHub Pages. CI has no Lakekeeper, so it uses the `docs` target in [profiles.yml](profiles.yml), which attaches an empty in-memory database as `lake`. Models still compile and the lineage graph is complete, but the catalog is empty: columns show the descriptions from the YAML files, without data types.

## dbt artifacts

The Dagster container sets `DBT_TARGET_PATH=target-docker`, so its artifacts go to `target-docker/` instead of `target/`. dbt's partial-parse file stores file paths, and one written on Windows breaks a parse in the Linux container. If you also run dbt on your machine, for example through an editor extension, the two don't collide. Git ignores both folders.
