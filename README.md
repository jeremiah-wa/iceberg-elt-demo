# Iceberg ELT demo

An ELT pipeline that keeps every table in Apache Iceberg. dlt loads issues and pull requests of five open-source repos from the GitHub API, dbt turns them into staging and mart tables, and Dagster runs both. Tables are Parquet files in MinIO, registered in Lakekeeper, an Iceberg REST catalog.

Everything runs in Docker. You don't need a GitHub token, cloud account or local Python.

```
GitHub REST API
      │  dlt
      ▼
raw.issues, raw.pull_requests            ┐
      │  dbt                             │  Iceberg tables in MinIO (s3://lake/lakekeeper),
      ▼                                  │  registered in Lakekeeper (warehouse "demo")
staging.stg_github__issues, ...          │
      │  dbt                             │
      ▼                                  │
marts.fct_pull_requests                  ┘

Dagster runs the dlt and dbt steps as one job.
```

## Quickstart

You need Docker with Compose v2, and ports 3000, 8181, 9000 and 9001 free.

```bash
git clone <repo-url> iceberg-elt-demo
cd iceberg-elt-demo/infra
docker compose up -d
```

The first start builds the Dagster image, which takes a few minutes. Then open Dagster at http://localhost:3000, go to **Assets** and click **Materialize all**. The run loads the latest 100 issues and pull requests per repo, builds the dbt models and runs the dbt tests.

| UI | URL | What to look at |
|---|---|---|
| Dagster | http://localhost:3000 | Runs, the asset graph from `raw` to `marts`, dbt tests as asset checks |
| MinIO console | http://localhost:9001 | Data and metadata files under `lake/lakekeeper`. Log in with `minio-root-user` / `minio-root-password` |
| Lakekeeper | http://localhost:8181 | Catalog UI with the `raw`, `staging` and `marts` namespaces |

To stop, run `docker compose down`. Add `-v` to also delete the data, the catalog and Dagster's run history.

## Repository layout

| Folder | What it holds | Details |
|---|---|---|
| `extract_load/` | dlt pipeline that loads GitHub issues and PRs into `raw` | [extract_load/README.md](extract_load/README.md) |
| `transform/` | dbt project that builds `staging` and `marts` | [transform/README.md](transform/README.md) |
| `orchestrate/` | Dagster definitions: assets, the `elt` job, a daily schedule | [orchestrate/README.md](orchestrate/README.md) |
| `infra/` | Docker Compose stack: MinIO, Lakekeeper, Dagster | [infra/README.md](infra/README.md) |

`extract_load`, `transform` and `orchestrate` form one [uv](https://docs.astral.sh/uv/) workspace with a single lockfile. Running `uv sync` in the repo root gives your editor an environment for autocomplete and type checking. The pipelines themselves run in the Dagster container. [infra/README.md](infra/README.md) explains why.
