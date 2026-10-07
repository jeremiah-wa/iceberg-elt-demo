# orchestrate

Dagster definitions for the demo, loaded as the code location `elt`. It turns the dlt pipeline and the dbt project into assets and runs them as one job.

## Files

| File | Contents |
|---|---|
| [definitions.py](definitions.py) | The `Definitions` object: assets, the `elt` job, a daily schedule, and the dlt and dbt resources |
| [assets/ingest.py](assets/ingest.py) | dlt assets for `raw/issues` and `raw/pull_requests`, built from [extract_load](../extract_load/README.md) with dagster-dlt |
| [assets/transform.py](assets/transform.py) | dbt assets for every model in [transform](../transform/README.md), built with dagster-dbt |
| [settings.py](settings.py) | Paths to the other projects and settings read from environment variables |

## Assets and lineage

Asset keys match the Iceberg table names: `raw/issues`, `staging/stg_github__issues`, `marts/fct_pull_requests` and so on. The dbt sources `github.issues` and `github.pull_requests` resolve to the same keys as the dlt assets, so Dagster draws one graph from the GitHub load to the mart. The dlt assets are in the `ingest` group and the dbt models in `transform`. dbt tests show up as asset checks.

## Job and schedule

`elt` materializes every asset: dlt first, then `dbt build`. **Materialize all** in the UI does the same.

`elt_schedule` runs the job daily at 06:00 UTC. It starts switched off. Turn it on under **Automation** in the UI.

## Running Dagster

`docker compose up -d` in [infra](../infra/README.md) starts `dagster dev` at http://localhost:3000. The container mounts this folder, `extract_load/` and `transform/`, so after editing code you click **Reload definitions** instead of rebuilding the image. Rebuild with `docker compose up -d --build` when dependencies in `uv.lock` change.

On load, `dagster dev` runs `dbt parse` to refresh dbt's manifest, so new or changed models appear after a reload.

## Implementation notes

- **dlt's working directory.** dlt reads `.dlt/config.toml` and `.dlt/secrets.toml` from the current directory by default. [assets/ingest.py](assets/ingest.py) sets `DLT_PROJECT_DIR` to `extract_load/` before importing the pipeline, so Dagster can run it from anywhere.
- **A fresh dlt source per run.** The asset builds a new source object for each run, rather than reusing the one from definition time. A dlt source holds generators, and a generator can only be consumed once.
- **dbt version.** dagster-dbt doesn't support dbt v2 yet, so the project pins dbt-core 1.12. See [transform/README.md](../transform/README.md#why-dbt-core-and-not-dbt-v2).
