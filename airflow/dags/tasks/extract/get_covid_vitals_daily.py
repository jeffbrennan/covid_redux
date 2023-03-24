import pandas as pd
from datetime import date
from datetime import datetime as dt
import sqlite3


def get_vitals(url, file_name):
    from datetime import date
    import pandas as pd

    sheetnames = pd.ExcelFile(url).sheet_names
    new_sheet_name = sheetnames[-1]
    current_year = str(dt.today().year)
    assert current_year in new_sheet_name

    raw_df = pd.read_excel(url, sheet_name=new_sheet_name, skiprows=2)
    df_cols = raw_df.columns.to_list()
    df_cols_parsed = [dt.strftime(i, '%m/%d/%Y') if isinstance(i, date) else i for i in df_cols]
    raw_df.columns = df_cols_parsed

    raw_df.to_parquet(f'./data_dump/{file_name}.parquet')


def write_db(df_name, table_name):
    import sqlite3
    import pandas as pd
    conn_stage = sqlite3.connect('db/staging.db')

    df = pd.read_parquet(f'./data_dump/{df_name}.parquet')
    df.to_sql(table_name, con=conn_stage, index=False, if_exists='replace')

