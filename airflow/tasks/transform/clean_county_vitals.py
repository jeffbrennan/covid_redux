import pandas as pd
import re
from datetime import datetime as dt
from datetime import timedelta
import sqlite3


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
    existing_vitals = pd.read_sql(
        '''
        select * 
        from main.county
        where date = (
                        select max(date) from main.county
                     )
        ''',
        con=prod_conn
    )

prod_conn = sqlite3.connect('db/prod.db')
stage_conn = sqlite3.connect('db/staging.db')
TODAY = dt.today()
