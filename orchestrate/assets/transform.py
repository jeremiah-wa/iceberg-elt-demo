from collections.abc import Mapping
from typing import Any

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from settings import DBT_TARGET_PATH, TRANSFORM_DIR

dbt_project = DbtProject(
    project_dir=TRANSFORM_DIR,
    profiles_dir=TRANSFORM_DIR,
    target_path=DBT_TARGET_PATH,
)
# Under `dagster dev`, rebuild target/manifest.json on load; otherwise it must already exist
dbt_project.prepare_if_dev()


class SchemaTableTranslator(DagsterDbtTranslator):
    """Keys sources and models as [schema, name], e.g. raw/issues and marts/fct_pull_requests,
    so the `github` sources resolve to the dlt assets."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        return AssetKey([dbt_resource_props["schema"], dbt_resource_props["name"]])

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return "transform"


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=SchemaTableTranslator(),
)
def transform_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
