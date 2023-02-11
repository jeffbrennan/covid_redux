import pandas as pd
from datetime import date
from datetime import datetime as dt
import sqlite3


def get_new_sheetname(url):
    sheetnames = pd.ExcelFile(url).sheet_names
    new_sheet_name = sheetnames[-1]
    assert str(TODAY.year) in new_sheet_name

    return new_sheet_name


def get_vitals(url):
    new_sheet_name = get_new_sheetname(url)
    raw_df = pd.read_excel(url, sheet_name=new_sheet_name, skiprows=2)
    df_cols = raw_df.columns.to_list()
    df_cols_parsed = [dt.strftime(i, '%m/%d/%Y') if isinstance(i, date) else i for i in df_cols]
    raw_df.columns = df_cols_parsed

    return raw_df


def manage_vitals():
    results = {'county_vitals_cases': get_vitals(case_url),
               'county_vitals_deaths': get_vitals(death_url)
               }
    return results


conn_stage = sqlite3.connect('db/staging.db')
conn_prod = sqlite3.connect('db/prod.db')

TODAY = dt.today()

dshs_base_url = 'https://www.dshs.texas.gov/sites/default/files/chs/data/COVID'
case_url = f'{dshs_base_url}/Texas%20COVID-19%20New%20Confirmed%20Cases%20by%20County.xlsx'
death_url = f'{dshs_base_url}/Texas%20COVID-19%20Fatality%20Count%20Data%20by%20County.xlsx'

results = manage_vitals()

for key, value in results.items():
    value.to_sql(f'{key}', con=conn_stage, index=False, if_exists='replace')
