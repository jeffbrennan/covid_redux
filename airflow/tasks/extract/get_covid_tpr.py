# region imports -----
import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import re
from datetime import datetime as dt
from datetime import timedelta
import sqlite3


# endregion

# region functions -----
def write_extracted_data(extracted_data):
    # overwrite w/ deterministic schema based on pulled columns
    extracted_data.to_sql('tpr_raw', con=stage_conn, if_exists='replace', index=None)


def extract_tpr_data(new_file_result):
    test_col_substrings = ['test', 'naat']
    out_columns = ['County', 'Date']
    metadata_columns = ['LAST_UPDATED']
    new_file_path = new_file_result['file_path']

    assert 'Counties' in pd.ExcelFile(new_file_path).sheet_names
    current_tpr_file = pd.read_excel(new_file_path, sheet_name='Counties', skiprows=1)

    tpr_all_cols = current_tpr_file.columns.tolist()
    test_columns = [i for i in tpr_all_cols if any(x in i.lower() for x in test_col_substrings)]

    out_columns.extend(test_columns)
    out_columns.extend(metadata_columns)

    # minimal cleaning to load into db
    extracted_tpr_df = (
        current_tpr_file
        .query("`State Abbreviation` == 'TX'")
        .assign(Date=new_file_result['last_updated'])
        .assign(LAST_UPDATED=dt.today())
        [out_columns]
    )
    return extracted_tpr_df


def check_if_tpr_new(tpr_file_date):
    #     TOOD: get max value from prod db to see if file is new
    db_current_date = tpr_file_date - timedelta(days=1)

    db_delta_days = (tpr_file_date - db_current_date).days
    today_delta_days = (dt.today() - tpr_file_date).days

    file_is_new = tpr_file_date > db_current_date

    assert file_is_new, f"File is not new. File last updated {today_delta_days} days ago."
    print(f'File is {today_delta_days} day(s) old and {db_delta_days} day(s) newer than the current database state.')


def get_tpr_file_url():
    page = requests.get(TPR_BASE_URL)
    assert page.status_code == 200

    html_page = page.content.decode("utf-8")
    soup = BeautifulSoup(html_page, "lxml")

    # look for xlsx file references in the script tags
    file_path_source = [i.text for i in soup.findAll('script') if 'xlsx' in i.text]
    assert len(file_path_source) == 1

    # remove js variable declaration syntax and convert string of files to json list
    file_path_source_parsed = json.loads(
        (
            file_path_source[0]
            .replace('var initialState =', '')
            .replace('\n   ;', '')
            .strip()
        )
    )['view']['attachments']

    # get first (newest) xlsx url from href tag
    file_path_newest = [i['href'] for i in file_path_source_parsed if 'xlsx' in i['href']][0]
    tpr_url_newest = f'https://beta.healthdata.gov{file_path_newest}'.replace(' ', '%20')
    tpr_path_parsed_date = dt.strptime(re.search('\d{8}', file_path_newest)[0], '%Y%m%d')

    output = {'file_path': tpr_url_newest, 'last_updated': tpr_path_parsed_date}

    return output


# endregion

# globals
stage_conn = sqlite3.connect('db/staging.db')
TPR_BASE_URL = 'https://healthdata.gov/Health/COVID-19-Community-Profile-Report/gqxm-d9w9'

# locate url
tpr_url_result = get_tpr_file_url()
check_if_tpr_new(tpr_url_result['last_updated'])

# parse
tpr_parsed = extract_tpr_data(tpr_url_result)

# load
write_extracted_data(tpr_parsed)
