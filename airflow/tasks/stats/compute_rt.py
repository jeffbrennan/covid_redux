# region imports -----
import pandas as pd
import sqlite3
import pyspark
import polars as pl


# endregion
def load_cases():
    # TODO: replace with load_cases() after prod db is in complete state
    # cases = pl.read_sql(
    #     """
    #     select County, [Date], Cases_Daily,
    #     TSA_Combined, PHR_Combined, Metro_Area,
    #     Population_DSHS
    #     from county_cases a
    #     left join county_names b
    #     on a.County = b.County
    # """,
    #     con=conn_prod
    # )

    cases = (
        pl.read_csv('https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/master/tableau/county.csv')
        [['County', 'Date', 'Cases_Daily']]
    )

    county_names = pl.read_sql('select * from main.county_names', connection_uri=conn_prod)
    cases_joined = cases.join(county_names, how='inner', on='County')

    # categorical_cols = ['County', 'TSA_Combined', 'PHR_Combined', 'Metro_Area']
    num_cols = ['Cases_Daily', 'Population_DSHS']
    output_df = (
        cases_joined
        .with_columns(
            [
                pl.col('Date').str.strptime(pl.Date, '%Y-%m-%d').cast(pl.Date),
                pl.col(num_cols).cast(pl.UInt32)
            ]
        )
    )

    return output_df


# prepare county into groups with name "Level" and "Level_Name"
# Level: County, TSA_Combined, PHR_Combined, Metro_Area, State
def create_stacked_df(cases, level_name):
    if level_name == 'State':
        stacked_df = (
            cases
            .groupby('Date')
            .agg(
                [
                    pl.col('Cases_Daily').sum().alias('Cases_Daily'),
                    pl.col('Population_DSHS').sum().alias('Population_DSHS')
                ]
            )
            .with_columns(
                [
                    pl.lit('Texas').alias('Level_Name'),
                    pl.lit('State').alias('Level')

                ]
            )
        )
    else:
        stacked_df = (
            cases
            .groupby([level_name, 'Date'])
            .agg(
                [
                    pl.col('Cases_Daily').sum().alias('Cases_Daily'),
                    pl.col('Population_DSHS').sum().alias('Population_DSHS')
                ]
            )
            .rename({level_name: 'Level_Name'})
            .with_columns(pl.lit(level_name).alias('Level'))
        )

    stacked_df_out = stacked_df[['Level', 'Level_Name', 'Date', 'Cases_Daily', 'Population_DSHS']]
    return stacked_df_out


def prep_rt_groups(cases):
    cases_formatted = (
        cases
        .rename({'County': 'Level_Name'})
        .with_columns(pl.lit('County').alias('Level'))
        [['Level', 'Level_Name', 'Date', 'Cases_Daily', 'Population_DSHS']]
    )

    rt_prepped_df = pl.concat(
        [
            cases_formatted,
            create_stacked_df(cases, 'TSA_Combined'),
            create_stacked_df(cases, 'PHR_Combined'),
            create_stacked_df(cases, 'Metro_Area'),
            create_stacked_df(cases, 'State')
        ]
    )

    return rt_prepped_df


def calculate_rt():
    # implement with spark
    rt_calculated_df = None
    return rt_calculated_df


def write_results(rt_results):
    rt_results.to_sql('county_rt', con=conn_prod)


def clean_stacked_cases(stacked_cases):
    cleaned_cases = stacked_cases
    return cleaned_cases


def prep_rt(cases):
    stacked_cases = prep_rt_groups(cases)
    cleaned_cases = clean_stacked_cases(stacked_cases)

    return cleaned_cases


conn_prod_pd = sqlite3.connect('db/prod.db')
# conn_stage = sqlite3.connect('db/staging.db')
conn_prod = f'sqlite://db/prod.db'
conn_stage = f'sqlite://db/staging.db'

cases = load_cases()
cleaned_cases = prep_rt(cases)
rt_results = calculate_rt(cases_cleaned)
write_results()
