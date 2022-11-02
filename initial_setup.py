import pandas as pd
import sqlite3
import os
#region setup -----
# os.chdir('covid_redux')
base_url = 'https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/master'
conn_stage = sqlite3.connect('db/staging.db')
conn_prod = sqlite3.connect('db/prod.db')
#endregion

#region helpers -----
# county lookup
county_lookup = pd.read_csv(f'{base_url}/tableau/county.csv')
county_lookup_final = county_lookup[['County', 'TSA_Combined', 'PHR_Combined', 'Population_DSHS']]
county_lookup_final.drop_duplicates(inplace=True)

county_lookup_final.to_sql('county_names', conn_stage, if_exists='replace', index=False)
# endregion

#region initial upload -----

#region counties -----
#region vitals -----
tableau_county_csv = pd.read_csv(f'{base_url}/tableau/county.csv')
tableau_county_csv.to_sql('county', con=conn_prod, if_exists='replace')

tableau_stacked_demo = pd.read_csv(f'{base_url}/tableau/stacked_demographics.csv')
tableau_stacked_demo.to_sql('stacked_demographics', con=conn_prod, if_exists='replace')

#endregion
#region vaccines -----


#endregion
#endregion

tableau_county_tpr = pd.read_csv(f'{base_url}/tableau/county_TPR.csv')
tableau_county_tpr.to_sql('county_TPR', con=conn_prod, if_exists='replace')
#region stats -----

tableau_stacked_rt = pd.read_csv(f'{base_url}/tableau/stacked_rt.csv')
tableau_stacked_rt.to_sql('stacked_rt', con=conn_prod, if_exists='replace')
#endregion

#endregion