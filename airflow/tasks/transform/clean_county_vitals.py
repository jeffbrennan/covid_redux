# region imports -----
import time
import pandas as pd
from datetime import datetime as dt
import sqlite3
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest


# endregion


def update_tbl_metadata(tbl_name, tbl_max_date):
    metadata_insert_query = f"""
        INSERT OR IGNORE INTO
        TBL_METADATA (TABLE_NAME, LAST_UPDATED, MAX_DATE, EXPECTED_REFRESH_DAYS)
        VALUES(
                '{tbl_name}',
                '{TODAY}',
                '{tbl_max_date}', 
                '7'
              );
        """

    metadata_update_query = f"""
        UPDATE TBL_METADATA 
        SET 
            LAST_UPDATED = '{TODAY}',
            MAX_DATE = '{tbl_max_date}'
        WHERE TABLE_NAME = '{tbl_name}'
        """
    conn_prod.execute(metadata_insert_query)
    conn_prod.commit()

    conn_prod.execute(metadata_update_query)
    conn_prod.commit()


def write_to_prod(df, tbl_name):
    df.to_sql(tbl_name, con=conn_prod, if_exists='append', index=False)

    tbl_max_date = max(df['Date'])
    update_tbl_metadata(tbl_name, tbl_max_date)


def run_diagnostics(df):
    context = gx.get_context()
    df_run_date = dt.strftime(df['Date'].max(), '%Y-%m-%d')
    run_name = f'{df_run_date}_PROD_LOAD_COUNTY_VITALS'

    batch_request = RuntimeBatchRequest(
        datasource_name="county_vitals",
        data_connector_name="county_vitals_connector",
        data_asset_name="COUNTY_VITALS",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"vitals_id": ""},
    )

    result = context.run_checkpoint(
        checkpoint_name="county_vital_prod_load_check",
        run_name=run_name,
        validations=[
            {"batch_request": batch_request}
        ],
    )

    context.build_data_docs()
    return result


def get_existing_values():
    existing_vitals = pd.read_sql(
        '''
        select
        County,
        Cases_Cumulative as Cases_Cumulative_OLD,
        Deaths_Cumulative as Deaths_Cumulative_OLD
        from main.county_vitals
        where date = (
                        select max(date) from main.county_vitals
                     )
        ''',
        con=conn_prod
    )

    return existing_vitals


def pivot_vitals(vitals_dict):
    pivoted_df = (vitals_dict['df']
                  .melt(id_vars='County',
                        value_vars=vitals_dict['df'].columns[1:].to_list(),
                        var_name='Date',
                        value_name=vitals_dict['vital_colname'],
                        )
                  )
    return pivoted_df


def clean_county_vitals(vitals_combined):
    bad_county_values = '|'.join(['unallocated', 'unknown', '-', 'total', 'incomplete'])

    county_names = (
        pd.read_sql("select distinct County from main.county_names",
                    con=conn_stage)
        ['County']
    )

    existing_cumulative_values = get_existing_values()
    name_diff = set(existing_cumulative_values['County']).difference(set(county_names))
    assert len(name_diff) == 0

    existing_vitals_date = (
        pd.read_sql(
            '''
                select max(Date) as vitals_max_dt
                from county_vitals
            ''',
            con=conn_prod
        )
        .assign(vitals_max_dt=lambda x: pd.to_datetime(x['vitals_max_dt']))
        .squeeze()
    )

    vitals_final = (
        vitals_combined
        .merge(county_names,
               how='inner',
               on='County'
               )
        .assign(Date=lambda x: pd.to_datetime(x['Date']))
        .query("Date > @existing_vitals_date")
        .merge(existing_cumulative_values,
               how='left',
               on='County'
               )
        .assign(Cases_Daily=lambda x: x['Cases_Daily'].fillna(0))
        .sort_values(by=['County', 'Date'])
        .assign(Cases_Daily_Cumsum=lambda x: x.groupby('County')['Cases_Daily'].cumsum())
        .assign(Cases_Cumulative=lambda x: x.Cases_Cumulative_OLD + x.Cases_Daily_Cumsum)
        .assign(Deaths_Daily=lambda x: x['Deaths_Daily'].fillna(0))
        .assign(Deaths_Daily_Cumsum=lambda x: x.groupby('County')['Deaths_Daily'].cumsum())
        .assign(Deaths_Cumulative=lambda x: x.Deaths_Cumulative_OLD + x.Deaths_Daily_Cumsum)
        .astype(
            {
                'Cases_Daily': 'uint32',
                'Deaths_Daily': 'uint32',
                'Cases_Cumulative': 'uint32',
                'Deaths_Cumulative': 'uint32'
            }
        )
    )
    return vitals_final


def get_staging_vitals():
    cases_raw = pd.read_sql('select * from county_vitals_cases', con=conn_stage)
    deaths_raw = pd.read_sql('select * from county_vitals_deaths', con=conn_stage)
    vitals_raw = {'cases':
                      {'df': cases_raw,
                       'vital_colname': 'Cases_Daily'},
                  'deaths':
                      {'df': deaths_raw,
                       'vital_colname': 'Deaths_Daily'}
                  }

    vitals_pivoted = {key: pivot_vitals(vitals_raw[key]) for key, _ in vitals_raw.items()}
    vitals_combined = (
        pd.merge(vitals_pivoted['cases'], vitals_pivoted['deaths'],
                 how='outer',
                 on=['County', 'Date']
                 )
        .dropna(subset=['County', 'Date'])
    )

    return vitals_combined


conn_prod = sqlite3.connect('db/prod.db')
conn_stage = sqlite3.connect('db/staging.db')
TODAY = dt.today()

staging_vitals = get_staging_vitals()
cleaned_vitals = clean_county_vitals(staging_vitals)
vital_diagnostics = run_diagnostics(cleaned_vitals)

if vital_diagnostics.success:
    write_to_prod(df=cleaned_vitals, tbl_name="county_vitals")
