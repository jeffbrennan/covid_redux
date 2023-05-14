import os
from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd
import polars as pl
from dagster import (
    ConfigurableIOManager,
    Definitions,
    load_assets_from_modules,
    build_input_context,
    build_output_context,
)
from dagster_dbt import dbt_cli_resource
from dotenv import load_dotenv
from typing import Union

from covid_dagster import assets
from covid_dagster.assets import DBT_PROFILES, DBT_PROJECT_PATH


# base class for routing input/output
class PostgresManager(ConfigurableIOManager):
    connection_string: str

    # step 1
    def handle_output(self, context, obj: Union[pd.DataFrame, pl.DataFrame]) -> None:
        table_name = context.metadata['table_name']
        schema = context.metadata['schema']

        self.write_dataframe_to_disk(schema=schema, table_name=table_name, dataframe=obj)
        self.write_table_to_db(
            conn=self.connection_string,
            schema=schema,
            table_name=table_name,
            dataframe=obj
        )

    # step 2
    def load_input(self, context) -> None:
        table_name = context.upstream_output.metadata['table_name']
        schema = context.upstream_output.metadata['schema']

        self.read_table_from_db(
            conn=self.connection_string,
            schema=schema,
            table_name=table_name
        )


class PandasManager(PostgresManager):

    @staticmethod
    def write_table_to_db(conn: str, schema: str, table_name: str, dataframe: pd.DataFrame) -> None:
        conn_engine = create_engine(conn)
        conn_engine.execute(f'drop table if exists {schema}.{table_name}  cascade')
        dataframe.to_sql(
            table_name,
            con=conn_engine,
            schema=schema,
            if_exists='append',
            index=False
        )

    @staticmethod
    def read_dataframe_from_disk(schema: str, table_name: str) -> pd.DataFrame:
        # file_path = self._get_path(table_name)
        df = pd.read_parquet(f'covid_dagster/storage/{schema}/{table_name}.parquet')
        return df

    @staticmethod
    def read_table_from_db(conn: str, schema: str, table_name: str) -> pd.DataFrame:
        conn_engine = create_engine(conn)
        df = pd.read_sql_table(table_name, schema=schema, con=conn_engine)
        return df

    @staticmethod
    def write_dataframe_to_disk(schema: str, table_name: str, dataframe: pd.DataFrame) -> None:
        Path(f'covid_dagster/storage/{schema}').mkdir(parents=True, exist_ok=True)
        dataframe.to_parquet(f'covid_dagster/storage/{schema}/{table_name}.parquet')


# polars based handling of i/o
class PolarsManager(PostgresManager):

    @staticmethod
    def read_dataframe_from_disk(schema: str, table_name: str) -> pl.DataFrame:
        df = pl.read_parquet(f'covid_dagster/storage/{schema}/{table_name}.parquet')
        return df

    @staticmethod
    def read_table_from_db(conn: str, schema: str, table_name: str) -> pl.DataFrame:
        query = f'select * from {schema}.{table_name}'
        df = pl.read_database(query=query, connection_uri=conn)
        return df

    @staticmethod
    def write_table_to_db(conn: str, schema: str, table_name: str, dataframe: pl.DataFrame) -> None:
        conn_engine = create_engine(conn)
        conn_engine.execute(f'drop table if exists {schema}.{table_name} cascade')

        # workaround for pyarrow error NotImplementedError dt.tz
        orig_date_colnames = dataframe.select(pl.col('date')).columns
        datetime_replacement_dict = {key: 'datetime64[ns]' for key in orig_date_colnames}

        (
            dataframe
            .to_pandas(types_mapper=pd.ArrowDtype)
            .astype(datetime_replacement_dict)
            .to_sql(
                schema=schema,
                name=table_name,
                con=conn_engine,
                if_exists='replace',
                index=False
            )
        )

        # TODO: make sql injection safe
        for datetime_col in orig_date_colnames:
            conn_engine.execute(
                'alter table '
                f'{schema}.{table_name} '
                f'alter column {datetime_col} '
                ' type date; '
            )

    @staticmethod
    def write_dataframe_to_disk(schema: str, table_name: str, dataframe: pl.DataFrame) -> None:
        Path(f'covid_dagster/storage/{schema}').mkdir(parents=True, exist_ok=True)
        dataframe.write_parquet(
            file=f'covid_dagster/storage/{schema}/{table_name}.parquet',
            compression='lz4'
        )


class DBTManager(ConfigurableIOManager):
    connection_string: str

    def handle_output(self, context, obj: pd.DataFrame) -> None:
        pass

    def load_input(self, context) -> pl.DataFrame:
        asset_key = context.asset_key
        if len(asset_key.path) != 3:
            raise ValueError(f'Expected asset key to have 3 parts, got {len(asset_key.path)}')
        schema = asset_key.path[1]
        table_name = asset_key.path[2]

        # polars implementation
        df = pl.read_database(
            query=f'select * from {schema}.{table_name}',
            connection_uri=self.connection_string
        )

        return df

class NoneManager(ConfigurableIOManager):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        pass

def test_my_io_manager_handle_output(manager):
    test_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
    context = build_output_context(
        name="def",
        step_key="123",
        metadata={
            'table_name': 'test',
            'schema': 'origin'
        }
        # dagster_type=PythonObjectDagsterType(pd.DataFrame)
    )
    manager.handle_output(context, test_df)


def test_my_io_manager_load_input(manager):
    context = build_input_context(
        upstream_output=build_output_context(name="def", step_key="123",
                                             metadata={'schema': 'origin', 'table_name': 'test'})
    )
    manager.load_input(context)


# region call resources -----
load_dotenv()
DEPLOYMENT_NAME = os.getenv("DAGSTER_DEPLOYMENT")
CONNECTION_STRING = os.getenv(f'{DEPLOYMENT_NAME}_CONN')

# test_my_io_manager_handle_output(PandasManager(connection_string=CONNECTION_STRING))
# test_my_io_manager_load_input(PandasManager(connection_string=CONNECTION_STRING))

resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    "pandas_io_manager": PandasManager(connection_string=CONNECTION_STRING),
    "dbt_io_manager": DBTManager(connection_string=CONNECTION_STRING),
    "polars_io_manager": PolarsManager(connection_string=CONNECTION_STRING),
    'none_io_manager': NoneManager()
}

defs = Definitions(assets=load_assets_from_modules([assets]), resources=resources)
# endregion
