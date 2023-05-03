# /tutorial_template/tutorial_dbt_dagster/assets/__init__.py
from dagster_dbt import load_assets_from_dbt_project
# from covid_dagster import dbt_io_manager
from dagster import file_relative_path, asset, Definitions, IOManager, io_manager
from datetime import datetime as dt
from datetime import date
import pandas as pd

from dagster import Config

from dagster import Output


# class PandasIOManager(IOManager):
#     def __init__(self, con_string: str):
#         self._con = con_string
#
#     def handle_output(self, context, obj):
#         # dbt handles outputs for us
#         pass
#
#     def load_input(self, context) -> pd.DataFrame:
#         """Load the contents of a table as a pandas DataFrame."""
#         table_name = context.asset_key.path[-1]
#         return pd.read_sql(f"SELECT * FROM {table_name}", con=self._con)
#
# @io_manager(config_schema={"con_string": str})
# def dbt_io_manager(context):
#     return PandasIOManager(context.resource_config["con_string"])


def get_vitals(url: str) -> pd.DataFrame:
    sheetnames = pd.ExcelFile(url, engine='openpyxl').sheet_names
    new_sheet_name = sheetnames[-1]
    current_year = str(dt.today().year)
    assert current_year in new_sheet_name

    raw_df = pd.read_excel(
        url,
        sheet_name=new_sheet_name,
        skiprows=2,
        engine='openpyxl'
    )

    df_cols = raw_df.columns.to_list()
    df_cols_parsed = [dt.strftime(i, '%m/%d/%Y') if isinstance(i, date) else i for i in df_cols]
    df_cols_parsed[0] = 'county'
    raw_df.columns = df_cols_parsed
    return raw_df


@asset(
    name="county_cases_raw",
    key_prefix=["dbt"],
    group_name="staging",
    metadata={"table_name": "county_cases_raw"},
    io_manager_key='pandas_to_postgres_io_manager'
)
def county_cases_raw(context) -> pd.DataFrame:
    case_url = "https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20New%20Confirmed%20Cases%20by%20County.xlsx"
    data = get_vitals(case_url)
    return data


@asset(
    name="county_deaths_raw",
    key_prefix=["dbt"],
    group_name="staging",
    metadata={"table_name": "county_deaths_raw"},
    io_manager_key='pandas_to_postgres_io_manager'
)
def county_deaths_raw(context) -> pd.DataFrame:
    death_url = "https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20Fatality%20Count%20Data%20by%20County.xlsx"
    data = get_vitals(death_url)
    return data


DBT_PROJECT_PATH = file_relative_path(__file__, "../../covid_dbt")
DBT_PROFILES = file_relative_path(__file__, "../../covid_dbt/config")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    key_prefix=["dbt"],
    io_manager_key="dbt_to_dbt_io_manager",
)
# {
#     "dbt_io_manager": dbt_io_manager.configured({'con_string': 'postgresql://jeffb@localhost:5432/covid'})
# }
# )

# downstream plot that references the materialized customers table
# @asset(
#    ins={"prod.county_vitals": AssetIn(key_prefix=["covid_pipeline"])},
#    group_name="prod",
# )
# def covid_dagster_test(vitals) -> pd.DataFrame:
#    vitals_cleaned = vitals.copy()
#    vitals_cleaned['dagster_test'] = 'dagster_test'
#    return vitals_cleaned
