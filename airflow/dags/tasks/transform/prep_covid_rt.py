# region imports -----
from datetime import timedelta, datetime
import polars as pl
import random
import epyestim.covid19 as covid19
import sqlite3

# endregion

def Write_Output(df):
    df.to_pandas().to_sql('covid_rt_prep', con=conn_prod_pandas, if_exists='replace', index=False)


# prepare county into groups with name "Level_Type" and "Level"
# Level: County, TSA_Combined, PHR_Combined, Metro_Area, State
def create_stacked_df(cases: pl.DataFrame, level_type: str) -> pl.DataFrame:
    if level_type == 'State':
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
                    pl.lit('Texas').alias('Level'),
                    pl.lit('State').alias('Level_Type')

                ]
            )
        )
    else:
        stacked_df = (
            cases
            .groupby([level_type, 'Date'])
            .agg(
                [
                    pl.col('Cases_Daily').sum().alias('Cases_Daily'),
                    pl.col('Population_DSHS').sum().alias('Population_DSHS')
                ]
            )
            .rename({level_type: 'Level'})
            .with_columns(pl.lit(level_type).alias('Level_Type'))
        )

    stacked_df_out = stacked_df[['Date', 'Level_Type', 'Level', 'Population_DSHS', 'Cases_Daily']]
    return stacked_df_out


def prep_rt_groups(cases: pl.DataFrame) -> pl.DataFrame:
    cases_formatted = (
        cases
        .rename({'County': 'Level'})
        .with_columns(pl.lit('County').alias('Level_Type'))
        [['Date', 'Level_Type', 'Level', 'Population_DSHS', 'Cases_Daily']]
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


def clean_stacked_cases(stacked_cases: pl.DataFrame) -> pl.DataFrame:
    recent_case_avg_df = (
        stacked_cases
        .with_columns(pl.col("Date").max().over("Level").alias("Date_Max"))
        .filter(pl.col("Date") > (pl.col('Date_Max') - timedelta(weeks=3)))
        .groupby(['Level_Type', 'Level'])
        .agg(
            [
                pl.col('Cases_Daily').mean().alias('Cases_Daily_Avg_3wk')
            ]
        )
    )

    cleaned_cases_df = (
        stacked_cases
        .join(recent_case_avg_df, how='inner', on=['Level_Type', 'Level'])
        .with_columns(pl.col('Cases_Daily').rolling_mean(window_size=7, closed='right').alias('Cases_MA_7day'))
        .with_columns(pl.col('Cases_MA_7day').cast(pl.Float32))
        .filter(pl.col('Date') >= datetime(2020, 3, 15))
        [['Date', 'Level_Type', 'Level', 'Cases_MA_7day', 'Population_DSHS']]
    )

    return cleaned_cases_df


def prep_rt(cases: pl.DataFrame) -> dict:
    stacked_cases = prep_rt_groups(cases)
    cleaned_cases = clean_stacked_cases(stacked_cases)
    return cleaned_cases


def load_cases(use_disk_file=False):
    case_cols = ['County', 'Date', 'Cases_Daily']
    num_cols = ['Cases_Daily', 'Population_DSHS']

    # TODO: replace with load_cases() after prod db is in complete state
    if use_disk_file:
        cases = pl.read_parquet('county_cases.parquet')[case_cols]
        county_names = pl.read_sql('select * from main.county_names', connection_uri=conn_prod)
        cases_joined = cases.join(county_names, how='inner', on='County')
    else:
        # cases_joined = pl.read_sql(
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
        county_names = pl.read_sql('select * from main.county_names', connection_uri=conn_prod)
        case_url = 'https://raw.githubusercontent.com/jeffbrennan/TexasPandemics/master/tableau/county.csv'
        cases_joined = (
            pl.read_csv(case_url)
            [case_cols]
            .join(county_names, how='inner', on='County')
        )

    output_df = (
        cases_joined
        .with_columns(
            [
                pl.col(num_cols).cast(pl.UInt32),
                pl.col('Date').str.strptime(pl.Date, fmt='%Y-%m-%d').cast(pl.Date),

            ]
        )
    )

    return output_df


conn_prod = f'sqlite://db/prod.db'
conn_prod_pandas = sqlite3.connect('db/prod.db')

cases = load_cases(use_disk_file=False)
cleaned_cases = prep_rt(cases)
Write_Output(cleaned_cases)