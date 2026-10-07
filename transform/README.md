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

- [profiles.yml](profiles.yml) loads DuckDB's `iceberg` extension and attaches the Lakekeeper warehouse `demo` as a database called `lake`. `data/transform.duckdb` is DuckDB's local session file. It holds no tables.
- [dbt_project.yml](dbt_project.yml) puts every model in the `lake` database, in the `staging` or `marts` schema. The [generate_schema_name](macros/generate_schema_name.sql) macro keeps those names as they are, instead of dbt's default `<target_schema>_staging`.
- Models use a custom `iceberg_table` materialization ([macros/iceberg_table.sql](macros/iceberg_table.sql)). dbt-duckdb's built-in `table` materialization builds a temporary table and renames it into place. DuckDB can't rename Iceberg tables created in the same transaction, and it doesn't support `CREATE OR REPLACE` on them. So `iceberg_table` drops the table, commits, and creates it again. While a model rebuilds, its table is missing for a moment.

## Why dbt-core and not dbt v2

dbt v2 ships with its own DuckDB adapter and `catalogs.yml` support for Iceberg REST catalogs. dagster-dbt doesn't support it yet ([dagster#34233](https://github.com/dagster-io/dagster/issues/34233)), and installing both breaks, because dagster-dbt depends on dbt-core 1.x and both packages provide the `dbt` command. dbt-core 1.12 is the newest version dagster-dbt accepts.

## Docs site

[.github/workflows/dbt-docs.yml](../.github/workflows/dbt-docs.yml) runs `dbt docs generate` on every push to `main` that touches `transform/` and publishes the result to GitHub Pages. CI has no Lakekeeper, so it uses the `docs` target in [profiles.yml](profiles.yml), which attaches an empty in-memory database as `lake`. Models still compile and the lineage graph is complete, but the catalog is empty: columns show the descriptions from the YAML files, without data types.

## dbt artifacts

The Dagster container sets `DBT_TARGET_PATH=target-docker`, so its artifacts go to `target-docker/` instead of `target/`. dbt's partial-parse file stores file paths, and one written on Windows breaks a parse in the Linux container. If you also run dbt on your machine, for example through an editor extension, the two don't collide. Git ignores both folders.
