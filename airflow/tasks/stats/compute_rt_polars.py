# region imports -----
import sqlite3
import time
from datetime import timedelta, datetime
import sqlalchemy

import pandas as pd
import polars as pl

import pyspark
from pyspark.sql import SparkSession
# from pyspark.sql.functions import

import random
import epyestim.covid19 as covid19


# endregion


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
            [[case_cols]]
            .join(county_names, how='inner', on='County')
        )

    output_df = (
        cases_joined
        .with_columns(
            [
                pl.col(num_cols).cast(pl.UInt32)
            ]
        )
    )

    return output_df


# prepare county into groups with name "Level_Type" and "Level"
# Level: County, TSA_Combined, PHR_Combined, Metro_Area, State
def create_stacked_df(cases, level_type):
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


def prep_rt_groups(cases):
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


def calculate_rt_spark(pandas_df):
    case_timeseries = pandas_df.set_index('Date')['Cases_MA_7Day']
    rt_results_raw = covid19.r_covid(case_timeseries)

    pandas_rt_result = (
        rt_results_raw
        .reset_index()
        .rename(
            columns=
            {'index': 'Date',
             'R_mean': 'rt',
             'Q0.025': 'ci_low',
             'Q0.975': 'ci_high'
             }
        )
        .assign(Level=pandas_df['Level'])
        .assign(Level_Type=pandas_df['Level_Type'])
        [['Date', 'Level_Type', 'Level', 'rt', 'ci_low', 'ci_high']]
    )
    return pandas_rt_result


def get_rt(cleaned_cases: pl.DataFrame) -> dict:
    spark = (
        SparkSession.builder
        .appName("covid_rt")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "10g")
        .getOrCreate()
    )

    spark_df = spark.createDataFrame(cleaned_cases.to_pandas())[['Date', 'Level_Type', 'Level', 'Cases_MA_7Day']]
    num_levels = 3
    # TODO: fix monotonic increasing issue
    # all_levels = spark_df.select('Level').distinct().toPandas().squeeze().to_list()
    # sample_levels = random.choices(all_levels, k=num_levels)
    sample_levels = ['Harris', 'Bexar', 'Travis']
    spark_df_sample = spark_df.filter(spark_df.Level.isin(sample_levels))

    result_schema = """
        Date date,
        Level_Type string,
        Level string,
        rt double,
        ci_low double,
        ci_high double 
    """

    start_time = time.time()
    rt_results = spark_df_sample \
        .groupBy('Level') \
        .applyInPandas(calculate_rt_spark, schema=result_schema)
    rt_results_pandas = rt_results.toPandas()
    rt_runtime = time.time() - start_time
    rt_runtime_avg = rt_runtime / num_levels

    output = {
        'result': rt_results_pandas,
        'stats': {'runtime': rt_runtime,
                  'runtime_avg': rt_runtime_avg
                  }
    }

    return output


def write_results(rt_results):
    # TODO: add metadata and diagnostics
    # rt_results.spark.to_spark_io(
    #     format="jdbc", mode="overwrite",
    #     dbtable="county_rt_test", url=conn_prod
    # )
    rt_results['result'].to_sql('county_rt_test', con=conn_prod_pandas, if_exists='replace', index=False)


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
        .filter(pl.col('Date') >= datetime(2020, 3, 15))
        [['Date', 'Level_Type', 'Level', 'Cases_MA_7day', 'Population_DSHS']]
        # .partition_by(['Level_Type', 'Level'], as_dict=True)
    )

    return cleaned_cases_df


def prep_rt(cases: pl.DataFrame) -> dict:
    stacked_cases = prep_rt_groups(cases)
    cleaned_cases = clean_stacked_cases(stacked_cases)

    return cleaned_cases

conn_prod_pandas = sqlite3.connect('db/prod.db')
conn_prod = f'sqlite://db/prod.db'


cases = load_cases(use_disk_file=True)
cleaned_cases = prep_rt(cases)
rt_results = get_rt(cleaned_cases)
write_results(rt_results)
