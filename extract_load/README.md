# extract_load

A dlt pipeline that loads issues and pull requests from the GitHub REST API into Iceberg tables in the `raw` namespace of the Lakekeeper catalog.

## What it loads

| Table | Contents |
|---|---|
| `raw.issues` | Issues, as GitHub's `/repos/{owner}/{repo}/issues` endpoint returns them, plus a `repo` column |
| `raw.pull_requests` | Pull requests from the same endpoint. `pull_request__merged_at` is set for merged PRs. |
| `raw.*__labels`, `raw.*__assignees`, ... | Child tables dlt creates from nested lists in the records |

The source takes the latest 100 items per repo (issues and PRs together, newest first) for apache/iceberg, dlt-hub/dlt, dbt-labs/dbt, dagster-io/dagster and lakekeeper/lakekeeper. Each run replaces the tables. To load other repos, edit `REPOS` in [github_pipeline.py](github_pipeline.py).

## Run it

Dagster runs this pipeline as the `raw/issues` and `raw/pull_requests` assets. To run it by hand, use the Dagster container (from `infra/`):

```bash
docker compose exec -w /app/extract_load dagster python github_pipeline.py
docker compose exec -w /app/extract_load dagster python github_pipeline.py --items-per-repo 300
```

## How it works

`github_issues_and_pull_requests` in [github_pipeline.py](github_pipeline.py) is the source. Its `repo_issues` resource pages through the issues endpoint for each repo. GitHub returns issues and pull requests together there, so two transformers split the same pages into `issues` and `pull_requests`, and each repo is fetched once. Column hints on `closed_at` and `pull_request__merged_at` keep those columns in the schema even when a load has no closed or merged items.

`pipeline.run(..., table_format="iceberg")` makes dlt write Iceberg tables through pyiceberg. [.dlt/config.toml](.dlt/config.toml) points dlt at:

- MinIO (`s3://lake/lakekeeper`, endpoint `http://localhost:9000`), where dlt also keeps its own state and schema files. The Dagster container passes the MinIO keys as environment variables.
- The Lakekeeper REST catalog (`http://localhost:8181/catalog`, warehouse `demo`). When dlt opens a table, Lakekeeper returns short-lived S3 credentials scoped to that table, and pyiceberg writes the data files with them.

## GitHub rate limits

The pipeline calls the API without a token. GitHub allows 60 anonymous requests per hour per IP address, and a run makes one request per 100 items per repo: 5 requests with the defaults. If a run fails with a 403 rate-limit error, wait an hour or add a token to `.dlt/secrets.toml`, which git ignores:

```toml
[sources.github]
access_token = "ghp_..."
```

A token raises the limit to 5,000 requests per hour. It only needs read access to public repositories.

## The `github/` folder

`github/` is dlt's verified GitHub source, added with `dlt init github filesystem` and left unchanged (see [github/README.md](github/README.md)). This pipeline only uses its `get_rest_pages` helper. The source's own `github_reactions` and `github_stargazers` use the GraphQL API, which always requires a token, and `github_repo_events` uses REST and works anonymously. The pipeline doesn't use any of them.
