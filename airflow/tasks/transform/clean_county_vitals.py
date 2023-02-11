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
        Cases_Daily as Cases_Daily_OLD,
        Deaths_Daily as Deaths_Daily_OLD
        from main.county_vitals
        where date = (
                        select max(date) from main.county_vitals
                     )
        ''',
        con=conn_prod
    )

    return existing_vitals


def clean_county_vitals(vitals_raw):
    bad_county_values = '|'.join(['unallocated', 'unknown', '-', 'total', 'incomplete'])
    final_cols = ['County', 'Date', 'Cases_Daily', 'Deaths_Daily']

    # pivot values

    # filter to only new values

    # clean & compute

    # return

    # cases_pivoted = (raw_cases
    #                  .melt(id_vars='County',
    #                        value_vars=raw_cases.columns[1:].to_list(),
    #                        var_name='Date',
    #                        value_name='Cases_Daily',
    #                        )
    #                  )


    vitals_clean = (
        vitals_raw[~vitals_raw['County'].str.lower().str.contains(bad_county_values)]
        .rename(
            {
                'Confirmed Cases': 'Cases_Daily',
                'Fatalities': 'Deaths_Daily'
            },
            axis='columns'
        )
        [final_cols]
    )

    existing_vitals = get_existing_values()
    vitals_combined = pd.concat([existing_vitals, vitals_clean])

    # TODO: switch from cases daily calc to cases cumulative calc using daily data source
    vitals_final = (
        vitals_clean.merge(existing_vitals,
                           how='left',
                           on='County'
                           )
        .astype(
            {
                'Cases_Daily': 'uint32',
                'Deaths_Daily': 'uint32'
            }
        )
        .assign(Date=lambda x: pd.to_datetime(x['Date']))
        # .sort_values(by=['County', 'Date'])
        .assign(Cases_Daily=lambda x: x['Cases_Daily'].fillna(0))
        .assign(Deaths_Daily=lambda x: x['Deaths_Daily'].fillna(0))
        .assign(Cases_Cumulative=lambda x: x['Cases_Daily'] + x['Cases_Cumulative_OLD'])
        .assign(Deaths_Cumulative=lambda x: x['Deaths_Daily'] + x['Deaths_Cumulative_OLD'])
        # .groupby('County')
        # .tail(1)
        .astype(
            {
                'Cases_Cumulative': 'uint32',
                'Deaths_Cumulative': 'uint32'
            }
        )
    )

    return vitals_final


conn_prod = sqlite3.connect('db/prod.db')
conn_stage = sqlite3.connect('db/staging.db')
TODAY = dt.today()

vitals_raw = pd.read_sql("select * from main.vitals", con=conn_stage)

cleaned_vitals = clean_county_vitals(vitals_raw)
vital_diagnostics = run_diagnostics(cleaned_vitals)

if vital_diagnostics.success:
    write_to_prod(df=cleaned_vitals, tbl_name="county_vitals")
