# How dbt talks to Iceberg

This page explains how this dbt project reads and writes Iceberg tables. It covers which service does what, what happens on the network when a model builds, where the files end up, and who holds which credentials. It's for anyone who has to change the dbt project or debug it. You should know what a dbt model is. You don't need to know Iceberg.

## The short version

dbt doesn't know Iceberg exists. dbt-duckdb hands every model's SQL to DuckDB, and DuckDB has the Iceberg catalog attached as a database called `lake`. When a model writes to `lake.staging.stg_github__issues`, DuckDB's `iceberg` extension does the Iceberg work:

- it asks Lakekeeper, the catalog, which tables exist and where their files are;
- it reads and writes Parquet files in MinIO directly;
- it tells Lakekeeper to commit the change.

Lakekeeper never sees the table data. It keeps the list of tables, hands out temporary MinIO keys, and accepts commits.

## Iceberg in one paragraph

An Iceberg table is a folder of files in object storage, plus one pointer. The data sits in Parquet files. Metadata files list which Parquet files belong to the table right now, and which belonged to it in earlier versions, called snapshots. The catalog keeps the pointer, which maps each table name to the location of its current metadata file. A write adds new files, then asks the catalog to move the pointer. Moving the pointer is the commit, and it's atomic, so readers see either the old version or the new one. In this stack, MinIO stores the files and Lakekeeper is the catalog.

## The services

```mermaid
flowchart LR
    subgraph dbtproc["dbt process"]
        direction TB
        dbt["dbt-core + dbt-duckdb"]
        duck["DuckDB<br/>iceberg + httpfs extensions"]
        dbt --> duck
    end

    subgraph catalog["Iceberg catalog"]
        lk["Lakekeeper :8181<br/>warehouse demo"]
        pg[("lakekeeper-db<br/>Postgres")]
        lk --> pg
    end

    minio[("MinIO :9000<br/>bucket lake")]

    duck -- "REST: tables, commits" --> lk
    duck -- "S3: Parquet files" --> minio
    lk -- "STS: temporary keys,<br/>metadata files" --> minio
```

| Service | What it does | Configured in |
|---|---|---|
| dbt-core | Works out the model order, renders the SQL, runs tests | [dbt_project.yml](../dbt_project.yml) |
| dbt-duckdb | The dbt adapter. It opens a DuckDB connection and sends it SQL. | [profiles.yml](../profiles.yml) |
| DuckDB | Runs the SQL in the same process as dbt. Its `iceberg` extension speaks to Lakekeeper and its `httpfs` extension speaks S3 to MinIO. | [profiles.yml](../profiles.yml) |
| Lakekeeper | The Iceberg REST catalog. It holds the table list and hands out temporary storage keys. | `infra/docker-compose.yml` |
| Postgres (`lakekeeper-db`) | Lakekeeper's own database: namespaces, tables, and the pointer to each table's current metadata file | `infra/docker-compose.yml` |
| MinIO | S3-compatible storage. Every data and metadata file lives in the `lake` bucket. | `infra/docker-compose.yml` |

dbt has to run in a container on the Docker Compose network. Lakekeeper tells clients to reach storage at `http://minio:9000`, and the name `minio` only resolves inside that network. Run from your own machine, dbt could reach Lakekeeper on `localhost:8181` but would fail to read or write any file.

## How dbt gets a `lake` database

DuckDB can have several databases open at once. dbt-duckdb opens two, both configured in [profiles.yml](../profiles.yml) under the `dev` target:

```mermaid
flowchart TB
    subgraph conn["One DuckDB connection"]
        main["main<br/>data/transform.duckdb<br/>(local file, no tables)"]
        lake["lake<br/>ATTACH 'demo' (TYPE iceberg)"]
    end
    lake -- "REST" --> lk["Lakekeeper<br/>warehouse demo"]
    lake -- "S3" --> minio[("MinIO")]
```

`path: data/transform.duckdb` is the main database. It's a local file that DuckDB needs for its session. No model is stored there.

The `attach` entry opens the second one. Each time dbt-duckdb opens a connection, it runs this:

```sql
ATTACH IF NOT EXISTS 'demo' AS lake (
    TYPE iceberg,
    ENDPOINT 'http://lakekeeper:8181/catalog',
    AUTHORIZATION_TYPE 'none'
);
```

| Option | Meaning |
|---|---|
| `path: demo` | The name of the Lakekeeper warehouse. For an Iceberg attach this is not a file path. |
| `alias: lake` | The name SQL uses. Every Iceberg table becomes `lake.<namespace>.<table>`. |
| `type: iceberg` | Use DuckDB's `iceberg` extension, which `extensions: [httpfs, iceberg]` loads. |
| `endpoint` | Lakekeeper's REST address inside the Compose network |
| `authorization_type: none` | Lakekeeper runs without login in this demo |

The attach goes straight through DuckDB. dbt-duckdb also has a plugin system, including an `iceberg` plugin built on PyIceberg, but this project doesn't use it. There's no `plugins:` key in the profile.

[dbt_project.yml](../dbt_project.yml) sets `+database: lake` for every model, so every model ends up in Iceberg. The [`generate_schema_name`](../macros/generate_schema_name.sql) macro keeps the schema names `staging` and `marts` as they are. Without it, dbt would build `main_staging` and `main_marts`.

Names line up like this:

| dbt | DuckDB | Lakekeeper |
|---|---|---|
| database `lake` | attached database `lake` | warehouse `demo` |
| schema `staging` | schema `lake.staging` | namespace `staging` |
| model `stg_github__issues` | table `lake.staging.stg_github__issues` | table `stg_github__issues` in namespace `staging` |
| source `github.issues` | table `lake.raw.issues` | table `issues` in namespace `raw` |

The `raw` tables must already be in the catalog when dbt runs. dbt only reads them.

## What happens when a model builds

Take `stg_github__issues`, which reads `raw.issues` and writes `staging.stg_github__issues`.

Every model uses a custom materialization, `iceberg_table` ([macros/iceberg_table.sql](../macros/iceberg_table.sql)), instead of dbt's built-in `table`. The built-in one builds the new table under a temporary name and renames it into place, which DuckDB doesn't allow for an Iceberg table created in the same transaction. DuckDB also rejects `CREATE OR REPLACE` on Iceberg tables, and it won't drop and create the same table in one transaction. So `iceberg_table` uses two transactions: one drops the old table, the other creates the new one.

The diagram below is simplified. It leaves out list calls, and it shows the create and the write as one step even though DuckDB may split them across several requests.

```mermaid
sequenceDiagram
    autonumber
    participant dbt as dbt-core
    participant duck as DuckDB
    participant lk as Lakekeeper
    participant pg as Postgres
    participant s3 as MinIO

    Note over dbt,duck: Connection opens
    duck->>lk: ATTACH: get config for warehouse demo
    lk-->>duck: catalog settings

    Note over dbt,s3: Transaction 1: drop the old table
    dbt->>duck: drop table if exists lake.staging.stg_github__issues
    duck->>lk: drop table
    lk->>pg: remove the table entry
    dbt->>duck: COMMIT

    Note over dbt,s3: Transaction 2: build the new table
    dbt->>duck: create table lake.staging.stg_github__issues as select ... from lake.raw.issues
    duck->>lk: load table raw.issues
    lk->>s3: STS: temporary keys for this table
    s3-->>lk: keys
    lk-->>duck: metadata file location + keys
    duck->>s3: read metadata, manifests, Parquet
    s3-->>duck: rows of raw.issues
    Note over duck: run the model's SQL in memory
    duck->>lk: create table staging.stg_github__issues
    lk-->>duck: table location + keys
    duck->>s3: write Parquet and manifest files
    dbt->>duck: COMMIT
    duck->>lk: commit the new snapshot
    lk->>s3: write the new metadata file
    lk->>pg: point the table at the new metadata file
    lk-->>duck: committed
```

A few things to take from this:

- The heavy work happens in DuckDB, inside the dbt process. Lakekeeper and MinIO only serve files and metadata.
- DuckDB reads and writes Parquet straight to MinIO, with keys that Lakekeeper got from MinIO's STS endpoint. The keys only work for the table they were issued for, and they expire.
- dbt's `ref()` and `source()` only produce names like `lake.raw.issues`. dbt never reads Iceberg metadata itself.
- Between the two transactions, the table doesn't exist. See [Known limits](#known-limits).

## Where things are stored

```mermaid
flowchart LR
    subgraph pg["Postgres (lakekeeper-db)"]
        ptr["staging.stg_github__issues<br/>→ current metadata file"]
    end
    subgraph s3["MinIO: s3://lake/lakekeeper/"]
        direction TB
        meta["metadata files<br/>(*.metadata.json)"]
        man["manifest files<br/>(lists of data files)"]
        data["data files<br/>(*.parquet)"]
        meta --> man --> data
    end
    ptr --> meta
```

| What | Where | Who writes it |
|---|---|---|
| Table names and namespaces | Postgres, through Lakekeeper | Lakekeeper, when DuckDB creates or drops a table |
| Pointer to each table's current metadata file | Postgres | Lakekeeper, on every commit |
| Metadata files (schema, snapshots) | MinIO, under `s3://lake/lakekeeper/` | Lakekeeper, on every commit |
| Manifest files and Parquet data files | MinIO, next to the metadata | DuckDB |
| DuckDB session file | `transform/data/transform.duckdb` | DuckDB. It holds no tables. |
| dbt artifacts (manifest, run results) | the dbt target folder, `transform/target/` unless `DBT_TARGET_PATH` says otherwise | dbt |

Lakekeeper picks the folder for each table under `s3://lake/lakekeeper`. To find a table's files, look up its location in the Lakekeeper UI at http://localhost:8181 instead of guessing a path from its name.

The warehouse `demo` was set up once, when the stack first started. Its config tells Lakekeeper which bucket and prefix to use, the MinIO endpoint, and the keys to use when it talks to MinIO.

## Who holds which keys

| Client | MinIO access | How it gets it |
|---|---|---|
| Lakekeeper | Root keys | Stored in the `demo` warehouse config |
| DuckDB | Temporary keys for one table | Lakekeeper requests them from MinIO's STS endpoint and returns them when DuckDB loads or creates a table |

This is why [profiles.yml](../profiles.yml) has no S3 secret. DuckDB never sees the MinIO root keys. The warehouse setting `sts-enabled: true` turns this on.

Nobody logs in to Lakekeeper (`authorization_type: none`). That's fine for a local demo and wrong for anything shared.

## The `docs` target in CI

`.github/workflows/dbt-docs.yml` runs `dbt docs generate --target docs` on GitHub Actions, where no Lakekeeper or MinIO is running. The `docs` target in [profiles.yml](../profiles.yml) attaches an empty in-memory database under the same name, `lake`:

```mermaid
flowchart LR
    subgraph dev["target dev (Compose network)"]
        l1["lake"] --> lk["Lakekeeper + MinIO"]
    end
    subgraph docs["target docs (CI)"]
        l2["lake"] --> mem["empty in-memory database"]
    end
```

Models still compile because `lake` exists, so the lineage graph is complete. The catalog page has no column types, because there are no real tables to read them from.

## Known limits

### The `iceberg_table` materialization

Every model uses `iceberg_table` because dbt-duckdb's built-in `table` materialization fails on Iceberg (see [What happens when a model builds](#what-happens-when-a-model-builds)). The first three limits below come from its drop-then-create sequence. The last three come from the materialization being a short custom macro that skips most of what the built-in one does.

- The table is missing during a rebuild. A query from another connection between the two commits fails with `Table with name ... does not exist!` instead of returning the old rows.
- A failed build leaves no table. If the create fails, the old table is already gone, and it stays gone until a later run succeeds. dbt skips the downstream models and tests.
- Snapshot history restarts on every run, because each run creates a new table. Iceberg time travel never reaches past the latest build.
- `iceberg_table` ignores `grants`, `persist_docs`, `indexes`, `partitioned_by` and `sorted_by`. A model that sets them gets no warning.
- Python models fail, because the materialization only declares SQL.
- There are no incremental models. Every run rebuilds every table.

The fix has to come from one of three places:

- DuckDB adding atomic replace for Iceberg tables;
- dbt-duckdb changing how it drops and renames Iceberg tables;
- a different dbt adapter whose engine replaces Iceberg tables in one commit, such as Trino or Spark.

### dbt-core 1.12 and dagster-dbt

The pipeline runs dbt through dagster-dbt, and dagster-dbt 0.29.25 requires `dbt-core<1.13`. So [pyproject.toml](../pyproject.toml) pins dbt-core 1.12, and the project can't move to dbt v2. Support for v2 in dagster-dbt is an open request, [dagster#34233](https://github.com/dagster-io/dagster/issues/34233).

Moving to dbt v2 wouldn't fix the rebuild problem anyway. Against an Iceberg REST catalog, v2's DuckDB `table` materialization also drops the table, commits, and creates it again. It would bring back grants and `persist_docs`, and add built-in incremental models.
