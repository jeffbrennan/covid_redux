import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import types

# region setup -----
base_url = 'https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/2023_dashboard_refresh'
conn_local = create_engine('postgresql://jeffb@localhost:5432/covid')
# endregion

# region helpers -----
# county lookup
county_lookup = pd.read_csv(f'{base_url}/tableau/county.csv')
county_lookup_final = (
    county_lookup[['County', 'TSA_Combined', 'PHR_Combined', 'Metro_Area']]
    .drop_duplicates()
    .rename(columns={'County': 'county', 'TSA_Combined': 'tsa', 'PHR_Combined': 'phr', 'Metro_Area': 'metro_area'})
)
county_lookup_final.to_sql('county_names', conn_local, schema='dbt', if_exists='replace', index=False)
# endregion

# county populations
county_populations = (
    county_lookup[['County', 'Date', 'Population_DSHS']]
    .drop_duplicates()
    .sort_values('Date')
    .groupby(['County', 'Population_DSHS'])
    .first()
    .reset_index()
    [['County', 'Date', 'Population_DSHS']]
    .sort_values(['County', 'Date'])
    .assign(start_date=lambda x: x['Date'],
            end_date=lambda x: x.groupby('County')['Date'].shift(-1)
            )
    .rename(columns={'County': 'county', 'Population_DSHS': 'population'})
    [['county', 'start_date', 'end_date', 'population']]
)

county_populations_types = {
    'county': types.VARCHAR(length=255),
    'start_date': types.DATE,
    'end_date': types.DATE,
    'population': types.INTEGER
}

county_populations.to_sql(
    'county_populations',
    conn_local,
    schema='dbt',
    if_exists='replace',
    index=False,
    dtype=county_populations_types
)

# region initial upload -----

# region counties -----
# region vitals -----

county_vitals = (
    county_lookup
    [['County', 'Date', 'Case_Type', 'Cases_Daily', 'Cases_Cumulative', 'Deaths_Daily', 'Deaths_Cumulative']]
    .query('Date < "2023-04-30"')
    .rename(
        columns={
            'County': 'county', 'Date': 'date', 'Case_Type': 'case_type',
            'Cases_Daily': 'cases_daily', 'Cases_Cumulative': 'cases_cumsum',
            'Deaths_Daily': 'deaths_daily', 'Deaths_Cumulative': 'deaths_cumsum'}
    )
)

county_cases = (
    county_vitals
    [['county', 'date', 'case_type', 'cases_daily', 'cases_cumsum']]
)

county_cases_types = {
    'county': types.VARCHAR(length=255),
    'date': types.DATE,
    'case_type': types.VARCHAR(length=23),  # confirmed or confirmed_plus_probable
    'cases_daily': types.INTEGER,
    'cases_cumsum': types.INTEGER,
}

county_cases.to_sql(
    'fct_county_cases',
    conn_local,
    schema='mart',
    if_exists='replace',
    index=False,
    dtype=county_cases_types
)

county_deaths = (
    county_vitals
    [['county', 'date', 'deaths_daily', 'deaths_cumsum']]
    .drop_duplicates()
)

county_deaths_types = {
    'county': types.VARCHAR(length=255),
    'date': types.DATE,
    'deaths_daily': types.INTEGER,
    'deaths_cumsum': types.INTEGER,
}

county_deaths.to_sql(
    'fct_county_deaths',
    conn_local,
    schema='mart',
    if_exists='replace',
    index=False,
    dtype=county_deaths_types
)

# endregion

# region vaccinations -----
county_vaccinations_raw = pd.read_csv(f'{base_url}/tableau/sandbox/county_daily_vaccine.csv')
max_vax_date = county_vaccinations_raw['Date'].max()

county_vaccinations = (
    county_vaccinations_raw
    .query('Vaccination_Type == "all"')
    [['County', 'Date', 'Doses_Administered', 'At_Least_One_Dose', 'Fully_Vaccinated', 'Boosted']]
    .rename(
        columns={
            'County': 'county',
            'Date': 'date',
            'Doses_Administered': 'vaccine_doses_administered',
            'At_Least_One_Dose': 'people_vaccinated_with_at_least_one_dose',
            'Fully_Vaccinated': 'people_fully_vaccinated',
            'Boosted': 'people_vaccinated_with_at_least_one_booster_dose'
        }
    )
    .query('date < @max_vax_date')
)

county_vaccinations_types = {
    'county': types.VARCHAR(length=255),
    'date': types.DATE,
    'vaccine_doses_administered': types.INTEGER,
    'people_vaccinated_with_at_least_one_dose': types.INTEGER,
    'people_fully_vaccinated': types.INTEGER,
    'people_vaccinated_with_at_least_one_booster_dose': types.INTEGER
}



county_vaccinations.to_sql(
    'fct_county_vaccinations',
    conn_local,
    schema='mart',
    if_exists='replace',
    index=False,
    dtype=county_vaccinations_types
)
# endregion
