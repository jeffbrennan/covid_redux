import pandas as pd
import sqlite3
#region setup -----
base_url = 'https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/master'
conn = sqlite3.connect('db/staging.db')
conn_prod = sqlite3.connect('db/prod.db')
#endregion
#region helpers -----

# county lookup
county_lookup = pd.read_csv(f'{base_url}/tableau/county.csv')
county_lookup_final = county_lookup[['County', 'TSA_Combined', 'PHR_Combined', 'Population_DSHS']]
county_lookup_final.drop_duplicates(inplace=True)

county_lookup_final.to_sql('county_names', conn, if_exists='replace', index=False)
# endregion

#region initial upload -----
tableau_county_csv = pd.read_csv(f'{base_url}/tableau/county.csv')
tableau_county_csv.to_sql('county', con=conn_prod)

#endregion