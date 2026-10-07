import argparse
from typing import Iterator, Optional, Sequence, Tuple

import dlt
from dlt.common.typing import TDataItems
from dlt.sources import DltResource

from github.helpers import get_rest_pages

# a tuple, not a list: dlt turns source arguments into a config dataclass, which rejects
# mutable defaults
REPOS = (
    ("apache", "iceberg"),
    ("dlt-hub", "dlt"),
    ("dbt-labs", "dbt"),
    ("dagster-io", "dagster"),
    ("lakekeeper", "lakekeeper"),
)


@dlt.source(name="github", section="github")
def github_issues_and_pull_requests(
    repos: Sequence[Tuple[str, str]] = REPOS,
    items_per_repo: int = 100,
    access_token: Optional[str] = None,
) -> Sequence[DltResource]:
    """Latest issues and pull requests of several public repos, from the GitHub REST API.

    Needs no token: anonymous calls allow 60 requests/hour per IP, and with the default
    `items_per_repo` a run makes one request per repo. An optional `access_token`
    (e.g. env SOURCES__GITHUB__ACCESS_TOKEN) raises the limit to 5,000/hour.
    Rows are tagged with `repo`, as GitHub doesn't include it.
    """

    @dlt.resource(selected=False)
    def repo_issues() -> Iterator[TDataItems]:
        # /issues returns issues and pull requests together, newest first
        for owner, name in repos:
            path = (
                f"/repos/{owner}/{name}/issues"
                f"?state=all&sort=created&direction=desc&per_page={min(items_per_repo, 100)}"
            )
            fetched = 0
            for page in get_rest_pages(access_token, path):
                page = page[: items_per_repo - fetched]
                yield [{**item, "repo": f"{owner}/{name}"} for item in page]
                fetched += len(page)
                if fetched >= items_per_repo:
                    break

    # Both read the same repo_issues pages, so each repo is fetched once. Column hints keep
    # columns that may be all-null in a load (e.g. no closed issues yet) in the schema.
    @dlt.transformer(
        data_from=repo_issues,
        write_disposition="replace",
        columns={"closed_at": {"data_type": "timestamp"}},
    )
    def issues(items: TDataItems) -> Iterator[TDataItems]:
        yield [item for item in items if "pull_request" not in item]

    @dlt.transformer(
        data_from=repo_issues,
        write_disposition="replace",
        columns={
            "closed_at": {"data_type": "timestamp"},
            "pull_request__merged_at": {"data_type": "timestamp"},
        },
    )
    def pull_requests(items: TDataItems) -> Iterator[TDataItems]:
        yield [item for item in items if "pull_request" in item]

    return repo_issues, issues, pull_requests


def github_pipeline() -> dlt.Pipeline:
    """Pipeline into the `raw` namespace of the Lakekeeper catalog (see .dlt/config.toml)."""
    return dlt.pipeline(
        pipeline_name="github",
        destination="filesystem",
        dataset_name="raw",
    )


def load_issues_and_pull_requests(items_per_repo: int = 100) -> None:
    """Loads issues and pull requests of REPOS into Iceberg tables `raw.issues` and `raw.pull_requests`"""
    pipeline = github_pipeline()
    data = github_issues_and_pull_requests(items_per_repo=items_per_repo)
    print(pipeline.run(data, table_format="iceberg"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--items-per-repo", type=int, default=100, help="latest issues + PRs per repo")
    args = parser.parse_args()
    load_issues_and_pull_requests(args.items_per_repo)
