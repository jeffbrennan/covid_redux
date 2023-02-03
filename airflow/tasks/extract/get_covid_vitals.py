import pandas as pd
import re
from datetime import datetime as dt
from datetime import timedelta
import sqlite3

prod_conn = sqlite3.connect('db/prod.db')
stage_conn = sqlite3.connect('db/staging.db')
# data updates at ~ 5PM EST
# if running after midnight (4 UTC) or before noon (16 UTC), subtract 1 day
# endregion
def clean_county_vitals():
    # setup
    vitals_raw = pd.read_sql("select * from main.vitals", con=stage_conn)
    county_names = pd.read_sql("select * from main.county_names", con=stage_conn)

    # assert 1 row = 1 county
    all_counties = county_names['County'].to_list()
    vitals_clean = vitals_raw[vitals_raw['County'].isin(all_counties)]

    # rename cols
    vitals_clean.rename({'Confirmed Cases': 'Cases_Cumulative',
                         'Fatalities': 'Fatalities_Cumulative'},
                        axis=1,
                        inplace=True
                        )

    # apply diagnostic checks
    check_nrow = vitals_clean.shape[0] == len(all_counties)
    check_colnames = vitals_clean.columns
    checks = [check_nrow, check_colnames]

    assert all(checks)

    # TODO: split into new function
    # compute daily cases
    existing_vitals = pd.read_sql('''select * 
                                     from main.county
                                     where date = (
                                        select max(date) from main.county
                                        )
                                  ''', con=prod_conn)


def get_county_vitals(county_url, county_sheetnames):
    vitals_sheetname = [s for s in county_sheetnames if "Case" in s and str(TODAY.year) in s]
    vitals_raw = pd.read_excel(county_url, vitals_sheetname)[vitals_sheetname[0]]
    vitals_date_raw = re.findall('\\d{1,2}/\\d{1,2}/\\d{4}', vitals_raw.columns[0])[-1]
    vitals_date_parsed = dt.strptime(vitals_date_raw, '%m/%d/%Y')

    if vitals_date_parsed == TODAY:
        vitals_header = vitals_raw.iloc[0]  # grab the first row for the header
        vitals_df = vitals_raw[1:]  # take the data less the header row
        vitals_df.columns = vitals_header  # set the header row as the df header
        vitals_df.loc[:, 'Date'] = dt.strftime(vitals_date_parsed, '%Y-%m-%d')

    else:
        print('Data not updated')
        vitals_df = None
    return vitals_df


def get_county_data():
    county_url = 'https://dshs.texas.gov/coronavirus/TexasCOVID19CaseCountData.xlsx'
    county_sheetnames = pd.ExcelFile(county_url).sheet_names

    # TODO: add recursive checks for delayed uploads here
    vital_results = get_county_vitals(county_url, county_sheetnames)
    output = {'vitals': vital_results}

    write_db(output, 'staging')
    return output


def write_db(raw_data_dict, conn=stage_con):
    # TODO: add name validation
    for key, value in raw_data_dict.items():
        value.to_sql(f'{key}', conn, if_exists='replace', index=False)


get_county_data()
get_county_vitals()
clean_county_vitals()
write_db()
