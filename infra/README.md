# infra

The Docker Compose stack for the demo: object storage, the Iceberg catalog and Dagster. Start it from this folder with `docker compose up -d`.

## Services

| Service | Image | Port | Role |
|---|---|---|---|
| `minio` | `pgsty/silo` | 9000 (API), 9001 (console) | S3-compatible storage. Tables live in the `lake` bucket. |
| `lakekeeper` | `quay.io/lakekeeper/catalog` | 8181 | Iceberg REST catalog and UI. Its warehouse `demo` stores tables under `s3://lake/lakekeeper`. |
| `lakekeeper-db` | `postgres:17` | none | Lakekeeper's metadata database |
| `dagster` | built from [dagster/Dockerfile](dagster/Dockerfile) | 3000 | `dagster dev`, which runs dlt and dbt |
| `create-bucket`, `lakekeeper-migrate`, `lakekeeper-init` | | | One-shot setup: create the bucket, migrate Lakekeeper's database, bootstrap Lakekeeper and create the warehouse from [lakekeeper/create-warehouse.json](lakekeeper/create-warehouse.json) |

MinIO no longer publishes Docker images, so the stack uses `pgsty/silo`, a maintained fork with the same API and `mc` client.

## Why the pipelines run in containers

When a client opens a table, Lakekeeper returns short-lived S3 credentials and the storage endpoint to use with them. That endpoint is `http://minio:9000`, the address Lakekeeper itself uses, and the name `minio` only resolves inside the Compose network. So dlt and dbt run in the `dagster` container, and your machine only opens the web UIs.

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
