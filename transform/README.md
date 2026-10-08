# transform

A dbt project that reads the raw GitHub tables and builds staging and mart tables. Every model is an Iceberg table in the Lakekeeper catalog. It uses dbt-core 1.12 with the dbt-trino adapter, so Trino does the compute.

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

To query the results, use the Trino CLI in the `trino` container:

```bash
docker compose exec trino trino --catalog lake --output-format ALIGNED \
  --execute "select repo, count(*) as prs, count_if(is_merged) as merged from marts.fct_pull_requests group by repo order by repo"
```

Leave out `--execute` for an interactive session. Trino needs no S3 keys here. Lakekeeper hands out short-lived credentials for each table it serves.

## How it writes Iceberg

[docs/architecture.md](docs/architecture.md) explains how dbt, Trino, Lakekeeper and MinIO work together, with diagrams of a model build, where the files are stored and who holds which keys. In short:

- [profiles.yml](profiles.yml) connects dbt-trino to the `trino` service. Trino's catalog `lake`, set up in [infra/trino/catalog/lake.properties](../infra/trino/catalog/lake.properties), is the Lakekeeper warehouse `demo`.
- [dbt_project.yml](dbt_project.yml) puts every model in the `lake` database, in the `staging` or `marts` schema. The [generate_schema_name](macros/generate_schema_name.sql) macro keeps those names as they are, instead of dbt's default `<target_schema>_staging`.
- Models use dbt's `table` materialization with `on_table_exists: replace`. dbt-trino rebuilds each table with one `CREATE OR REPLACE TABLE ... AS`, and Trino commits the new rows as a new snapshot of the same Iceberg table.

> [!NOTE]
> Rebuilds are atomic. Readers see the old table until Trino commits the new snapshot, and a failed build leaves the old table in place. Each table keeps its snapshot history, so time travel reaches earlier builds. That history costs storage. Every build keeps its files until someone expires old snapshots with Trino's `expire_snapshots`. [docs/iceberg-materialization.md](docs/iceberg-materialization.md) has the tested details and why the project moved off dbt-duckdb. [docs/iceberg-adapter-options.md](docs/iceberg-adapter-options.md) compares this with the other options.

## Why dbt-core and not dbt v2

dbt v2 has been generally available since September 2026, but dagster-dbt isn't ready for it. Version 0.29.25 requires `dbt-core<1.13`, its dbt Fusion support is in preview ([Dagster docs](https://docs.dagster.io/integrations/libraries/dbt/dbt-fusion)), and dbt v2 support is an open request ([dagster#34233](https://github.com/dagster-io/dagster/issues/34233)). dbt-core and v2 both provide a `dbt` command, and dbt-core's can shadow the v2 binary ([dagster#33513](https://github.com/dagster-io/dagster/issues/33513)). Dagster's docs suggest linking the v2 binary as `dbtf`, which `DbtCliResource` picks ahead of `dbt` when it's on `PATH`. dbt-core 1.12 is the newest version dagster-dbt accepts.

v2 wouldn't have fixed rebuilds on DuckDB either. Its DuckDB adapter drops an Iceberg table, commits and creates it again. [docs/iceberg-materialization.md](docs/iceberg-materialization.md#why-not-dbt-duckdb) links the v2 source.

## Docs site

[.github/workflows/dbt-docs.yml](../.github/workflows/dbt-docs.yml) runs `dbt docs generate` on every push to `main` that touches `transform/` and publishes the result to GitHub Pages. CI has no Trino, so it uses the `docs` target in [profiles.yml](profiles.yml) with `--empty-catalog` and `--no-populate-cache`, and dbt never connects. Models still compile and the lineage graph is complete, but the catalog is empty: columns show the descriptions from the YAML files, without data types.

## dbt artifacts

The Dagster container sets `DBT_TARGET_PATH=target-docker`, so its artifacts go to `target-docker/` instead of `target/`. dbt's partial-parse file stores file paths, and one written on Windows breaks a parse in the Linux container. If you also run dbt on your machine, for example through an editor extension, the two don't collide. Git ignores both folders.
