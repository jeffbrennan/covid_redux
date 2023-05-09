from dagster_dbt import load_assets_from_dbt_project
from dagster import file_relative_path

# cases (confirmed, probable) and deaths
from covid_dagster.assets.load_vitals import raw_county_cases_confirmed, raw_county_cases_probable, raw_county_deaths

# rt calculation
from covid_dagster.assets.compute_rt import rt_results

# vaccinations
from covid_dagster.assets.load_vaccinations import raw_county_vaccinations

DBT_PROJECT_PATH = file_relative_path(__file__, "../../covid_dbt")
DBT_PROFILES = file_relative_path(__file__, "../../covid_dbt/config")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    key_prefix=["covid"],
    io_manager_key="dbt_io_manager",
)
