from dagster_dbt import load_assets_from_dbt_project
from dagster import file_relative_path

# cases (confirmed, probable) and deaths
import covid_dagster.assets.load_vitals

DBT_PROJECT_PATH = file_relative_path(__file__, "../../covid_dbt")
DBT_PROFILES = file_relative_path(__file__, "../../covid_dbt/config")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    key_prefix=["dbt"],
    io_manager_key="dbt_to_dbt_io_manager",
)