import datetime

import pandas as pd
from datetime import datetime as dt
import requests
from dagster import asset
from pathlib import Path


def save_vaccine_file(vax_info: dict[str, str]) -> None:
    Path(vax_info['file_path']).parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(vax_info['url'])
    with open(vax_info['file_path'], 'wb') as output:
        output.write(response.content)


def get_vaccine_file(date_out: datetime.date) -> dict[str, str]:
    date_out_formatted = dt.strftime(date_out, '%Y-%m-%d')
    vaccine_base_url = 'https://www.dshs.texas.gov/sites/default/files/LIDS-Immunize-COVID19/COVID%20Dashboard/County%20Dashboard/COVID-19%20Vaccine%20Data%20by%20County'
    vaccine_date_out = dt.strftime(date_out, '%Y%m%d')

    vaccine_county_dshs_url = f'{vaccine_base_url}%20{vaccine_date_out}.xlsx'
    file_path_out = f'covid_dagster/storage/origin/vaccinations/{date_out_formatted}_vaccination_file.xlsx'

    output = {
        'file_path': file_path_out,
        'url': vaccine_county_dshs_url
    }
    return output


def get_county_sheet(file_path_out: str) -> list[str]:
    df_sheet_names = pd.ExcelFile(file_path_out, engine='openpyxl').sheet_names
    county_sheet = [i for i in df_sheet_names if 'County' in i and all(j not in i for j in ['Race', 'Age'])]
    assert len(county_sheet) == 1
    return county_sheet

def read_vaccine_file(file_path_out: str, date_out: dt.date) -> pd.DataFrame:
    county_sheet = get_county_sheet(file_path_out)

    column_names = pd.read_excel(
        file_path_out,
        sheet_name=county_sheet[0],
        nrows=1,
        engine='openpyxl'
    ).columns

    raw_df = pd.read_excel(
        file_path_out,
        sheet_name=county_sheet[0],
        skiprows=3,
        engine='openpyxl'
    )

    raw_df.columns = column_names
    raw_df['date'] = dt.strftime(date_out, '%Y-%m-%d')
    return raw_df

@asset(
    name="raw_county_vaccinations",
    key_prefix=["origin"],
    group_name="origin",
    metadata={
        "schema": "origin",
        "table_name": "raw_county_vaccinations",
    },
    io_manager_key='pandas_io_manager'
)
def raw_county_vaccinations() -> pd.DataFrame:
    date_out = dt.strptime('20230503', '%Y%m%d')
    vax_info = get_vaccine_file(date_out)
    save_vaccine_file(vax_info)
    df = read_vaccine_file(vax_info['file_path'], date_out)
    return df


raw_county_vaccinations()
