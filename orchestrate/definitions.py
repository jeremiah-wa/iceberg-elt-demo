from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource

from assets.ingest import github_assets
from assets.transform import dbt_project, transform_assets

elt_job = define_asset_job(
    "elt",
    selection=AssetSelection.all(),
    description="GitHub -> raw (dlt) -> staging -> marts (dbt), all Iceberg tables",
)

defs = Definitions(
    assets=[github_assets, transform_assets],
    jobs=[elt_job],
    schedules=[ScheduleDefinition(job=elt_job, cron_schedule="0 6 * * *")],
    resources={
        "dlt": DagsterDltResource(),
        "dbt": DbtCliResource(project_dir=dbt_project),
    },
)
