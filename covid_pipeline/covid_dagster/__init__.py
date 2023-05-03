from typing import Any

import pandas as pd
from dagster import ConfigurableIOManager
from dagster import Definitions, load_assets_from_modules
from dagster import (
    build_input_context,
    build_output_context,
)
from dagster_dbt import dbt_cli_resource
from sqlalchemy import create_engine

from covid_dagster import assets
from covid_dagster.assets import DBT_PROFILES, DBT_PROJECT_PATH


class ParquetToPostgresManager(ConfigurableIOManager):
    def write_table_to_db(self, table_name: str, dataframe: pd.DataFrame) -> None:
        # TODO: make this dynamic to handle local/prod envs
        # TODO: handle different types of uplaoad

        # initial staging tables - drop and replace cascading (b/c input cols can change)
        # downstream proper tables - append only (no truncate)

        conn_local = create_engine('postgresql://jeffb@localhost:5432/covid')
        conn_local.execute(f'drop table if exists dbt.{table_name}  cascade')
        dataframe.to_sql(
            table_name,
            con=conn_local,
            schema='dbt',
            if_exists='append',
            index=False
        )

    def read_dataframe_from_disk(self, table_name: str) -> pd.DataFrame:
        # file_path = self._get_path(table_name)
        df = pd.read_parquet(f'covid_dagster/storage/{table_name}.parquet')
        return df

    def write_dataframe_to_disk(self, table_name: str, dataframe: pd.DataFrame) -> None:
        # file_path = self._get_path(table_name)
        dataframe.to_parquet(f'covid_dagster/storage/{table_name}.parquet')

    def read_table_from_db(self, table_name: str) -> pd.DataFrame:
        conn_local = create_engine('postgresql://jeffb@localhost:5432/covid')
        df = pd.read_sql_table(table_name, schema='dbt', con=conn_local)
        return df

    # step 1
    def handle_output(self, context, obj: pd.DataFrame) -> None:
        table_name = context.metadata['table_name']
        self.write_dataframe_to_disk(table_name=table_name, dataframe=obj)
        self.write_table_to_db(table_name=table_name, dataframe=obj)

    # step 2
    def load_input(self, context) -> None:
        table_name = context.upstream_output.metadata['table_name']
        # self.read_dataframe_from_disk(table_name=table_name)
        self.read_table_from_db(table_name=table_name)


class DBTManager(ConfigurableIOManager):
    def handle_output(self, context, obj: Any) -> None:
        print('Handling output!')
        pass

    def load_input(self, context) -> Any:
        print('Handling input!')
        pass


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


def test_my_io_manager_handle_output(manager):
    test_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    context = build_output_context(
        name="def",
        step_key="123",
        metadata={'table_name': 'test'}
        # dagster_type=PythonObjectDagsterType(pd.DataFrame)
    )
    manager.handle_output(context, test_df)


def test_my_io_manager_load_input(manager):
    context = build_input_context(
        upstream_output=build_output_context(name="def", step_key="123", metadata={'table_name': 'test'})
    )
    manager.load_input(context)


# test_my_io_manager_handle_output(ParquetToPostgresManager())
# test_my_io_manager_load_input(ParquetToPostgresManager())

# region call resources -----
resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    "pandas_to_postgres_io_manager": ParquetToPostgresManager(),
    'dbt_to_dbt_io_manager': DBTManager()
}
defs = Definitions(assets=load_assets_from_modules([assets]), resources=resources)
# endregion