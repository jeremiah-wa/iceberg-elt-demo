# infra

The Docker Compose stack for the demo: object storage, the Iceberg catalog, Trino and Dagster. Start it from this folder with `docker compose up -d`.

## Prerequisites

- Docker Engine with the Compose plugin, or Docker Desktop. The commands use `docker compose`, not the older `docker-compose`.
- Free ports on your machine: 3000 (Dagster), 8080 (Trino), 8181 (Lakekeeper), 9000 and 9001 (MinIO).
- Internet access to pull the images, to build the Dagster image from PyPI packages, and for the pipeline's calls to the GitHub API. The calls are anonymous. GitHub allows 60 an hour per IP address, and a run makes five, one per repo.

You don't need a GitHub token, a cloud account or Python on your machine.

## System requirements

Measured on 2026-10-08 with Docker Desktop on Windows 11, with 16 CPUs and 8 GB of memory available to Docker:

| Resource | Recommended | Measured |
|---|---|---|
| Memory available to Docker | 6 GB | 2.9 GB with the stack idle, 3.6 GB at the peak of a full pipeline run. Trino held about 2 GB of that and may grow to its 3 GB limit. Dagster went from 0.7 GB to 1.2 GB during the run. |
| Disk | 6 GB | 6 GB of images, mostly the Dagster and Trino images at 2.4 GB each. The data volumes stayed under 100 MB. |
| CPU | | The stack sets no CPU limits. A full pipeline run took 31 s. |

On Docker Desktop, the memory available to Docker is set under **Settings > Resources**. With the WSL 2 backend on Windows, it's the `memory` setting in `.wslconfig` instead.

## Services

| Service | Image | Port | Role |
|---|---|---|---|
| `minio` | `pgsty/silo` | 9000 (API), 9001 (console) | S3-compatible storage. Tables live in the `lake` bucket. |
| `lakekeeper` | `quay.io/lakekeeper/catalog` | 8181 | Iceberg REST catalog and UI. Its warehouse `demo` stores tables under `s3://lake/lakekeeper`. |
| `lakekeeper-db` | `postgres:17` | none | Lakekeeper's metadata database |
| `trino` | `trinodb/trino` | 8080 | Query engine. dbt-trino runs every model here, through the catalog `lake` in [trino/catalog/lake.properties](trino/catalog/lake.properties). |
| `dagster` | built from [dagster/Dockerfile](dagster/Dockerfile) | 3000 | `dagster dev`, which runs dlt and dbt |
| `create-bucket`, `lakekeeper-migrate`, `lakekeeper-init` | | | One-shot setup: create the bucket, migrate Lakekeeper's database, bootstrap Lakekeeper and create the warehouse from [lakekeeper/create-warehouse.json](lakekeeper/create-warehouse.json) |

MinIO no longer publishes Docker images, so the stack uses `pgsty/silo`, a maintained fork with the same API and `mc` client.

## Why the pipelines run in containers

When a client opens a table, Lakekeeper returns short-lived S3 credentials and the storage endpoint to use with them. That endpoint is `http://minio:9000`, the address Lakekeeper itself uses, and the name `minio` only resolves inside the Compose network. So dlt and dbt run in the `dagster` container, Trino runs dbt's SQL in its own container, and your machine only opens the web UIs.

## Trino

[trino/catalog/lake.properties](trino/catalog/lake.properties) gives Trino an Iceberg catalog, `lake`, for the Lakekeeper warehouse `demo`. Trino takes the S3 credentials Lakekeeper vends for each table, so the file has no keys, only MinIO's endpoint, region and path-style access. The catalogs that ship with the image, such as `tpch`, stay available.

The image sizes the JVM heap at 80% of the container's memory, so `mem_limit: 3g` caps both. Trino used about 2 GB in testing, which left too little room under a 2 GB limit.

To run SQL by hand:

```bash
docker compose exec trino trino --catalog lake
```

The UI at http://localhost:8080 lists recent queries, including the ones dbt sent.

## The Dagster image

[dagster/Dockerfile](dagster/Dockerfile) installs the uv workspace from `uv.lock`. Compose mounts `extract_load/`, `transform/` and `orchestrate/` over the copies in the image, so code edits reach the container without a rebuild. Rebuild after dependency changes:

```bash
docker compose up -d --build
```

The container starts `dagster dev` from `/opt/dagster` with [dagster/workspace.yaml](dagster/workspace.yaml), not from `/app/orchestrate`. Dagster commands load a `.env` file from their working directory, and a `.env` in `orchestrate/` on your machine would otherwise leak into the container. Run history is kept in the `dagster-home` volume.

## Configuration

Every setting in [docker-compose.yml](docker-compose.yml) has a default, so the stack runs without a `.env` file. To override one, copy [.env.example](.env.example) to `.env` and uncomment it. The MinIO credentials also appear in `lakekeeper/create-warehouse.json`, so change both together. The warehouse is only created on first start, so after changing credentials, reset with `docker compose down -v`.

## Stop and reset

```bash
docker compose down      # stop, keep the data
docker compose down -v   # stop and delete MinIO data, the catalog and Dagster's run history
```

After `down -v`, the next `docker compose up -d` creates an empty bucket and warehouse again.
