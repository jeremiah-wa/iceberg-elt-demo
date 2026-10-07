import os

from dagster import AssetExecutionContext, AssetKey, AssetSpec
from dagster_dlt import DagsterDltResource, DagsterDltTranslator, dlt_assets
from dagster_dlt.translator import DltResourceTranslatorData

from settings import EXTRACT_LOAD_DIR

# dlt reads .dlt/config.toml and .dlt/secrets.toml from its project dir, by default the cwd
os.environ.setdefault("DLT_PROJECT_DIR", str(EXTRACT_LOAD_DIR))

from github_pipeline import github_issues_and_pull_requests, github_pipeline  # noqa: E402


class RawTableTranslator(DagsterDltTranslator):
    """Keys each asset like the Iceberg table it loads, e.g. raw/issues, so dbt sources match."""

    def get_asset_spec(self, data: DltResourceTranslatorData) -> AssetSpec:
        return super().get_asset_spec(data).replace_attributes(
            key=AssetKey([data.pipeline.dataset_name, data.resource.name]),
            deps=[],
        )


@dlt_assets(
    dlt_source=github_issues_and_pull_requests(),
    dlt_pipeline=github_pipeline(),
    name="github",
    group_name="ingest",
    dagster_dlt_translator=RawTableTranslator(),
)
def github_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    # a fresh source per run: its resources are generators that can only be consumed once
    yield from dlt.run(
        context=context,
        dlt_source=github_issues_and_pull_requests(),
        table_format="iceberg",
    )
