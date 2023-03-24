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


def load_cases(use_disk_file=False):
    if use_disk_file:
        cleaned_cases = pl.read_csv('data/cleaned_cases.csv')
    else:
        cleaned_cases = (
            pl.read_sql_table('cleaned_cases', conn_prod)
            .sort(['Level_Type', 'Level', 'Date'])
        )
    return cleaned_cases

conn_prod_pandas = sqlite3.connect('db/prod.db')
conn_prod = f'sqlite://db/prod.db'

cleaned_cases = load_cases(use_disk_file=True)
rt_results = get_rt(cleaned_cases)
write_results(rt_results)
