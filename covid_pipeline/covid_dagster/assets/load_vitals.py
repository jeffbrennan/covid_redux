from datetime import date
from datetime import datetime as dt

import pandas as pd
from dagster import asset


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
    name="raw_county_cases_confirmed",
    key_prefix=["dbt"],
    group_name="staging",
    metadata={"table_name": "raw_county_cases_confirmed"},
    io_manager_key='pandas_to_postgres_io_manager'
)
def raw_county_cases_confirmed(context) -> pd.DataFrame:
    case_url = "https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20New%20Confirmed%20Cases%20by%20County.xlsx"
    data = get_vitals(case_url)
    return data

@asset(
    name="raw_county_cases_probable",
    key_prefix=["dbt"],
    group_name="staging",
    metadata={"table_name": "raw_county_cases_probable"},
    io_manager_key='pandas_to_postgres_io_manager'
)
def raw_county_cases_probable(context) -> pd.DataFrame:
    case_url = "https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20New%20Probable%20Cases%20by%20County.xlsx"
    data = get_vitals(case_url)
    return data


@asset(
    name="raw_county_deaths",
    key_prefix=["dbt"],
    group_name="staging",
    metadata={"table_name": "raw_county_deaths"},
    io_manager_key='pandas_to_postgres_io_manager'
)
def raw_county_deaths(context) -> pd.DataFrame:
    death_url = "https://www.dshs.texas.gov/sites/default/files/chs/data/COVID/Texas%20COVID-19%20Fatality%20Count%20Data%20by%20County.xlsx"
    data = get_vitals(death_url)
    return data
