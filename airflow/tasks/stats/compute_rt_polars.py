# region imports -----
import sqlite3
import time
import polars as pl
import epyestim.covid19 as covid19


# endregion
def write_results(rt_results):
    (
        rt_results['result']
        .to_pandas()
        .to_sql(
            'county_rt_test_polars',
            con=conn_prod_pandas,
            if_exists='replace',
            index=False
        )
    )


def get_case_timeseries(cases: pl.Series, dates: pl.Series) -> pl.Series:
    return cases.to_pandas().set_axis(dates.to_pandas())


def calculate_rt(df: pl.DataFrame) -> pl.DataFrame:
    print(df.get_column('Level')[0])

    case_timeseries = get_case_timeseries(df.get_column('Cases_MA_7day'), df.get_column('Date'))
    rt_results_raw = covid19.r_covid(case_timeseries)

    polars_rt_result = (
        pl.from_pandas(rt_results_raw.reset_index())
        .rename({'index': 'Date',
                 'R_mean': 'rt',
                 'Q0.025': 'ci_low',
                 'Q0.975': 'ci_high'
                 }
                )
        [['Date', 'rt', 'ci_low', 'ci_high']]
    )

    final_result = (
        df.
        join(other=polars_rt_result, on='Date', how='left')
    )

    return final_result


def get_rt(cleaned_cases: pl.DataFrame) -> dict:
    sample_levels = ['Harris', 'Bexar', 'Travis']
    num_levels = len(sample_levels)

    cases_df_sample = (
        cleaned_cases
        .filter(pl.col('Level').is_in(sample_levels))
    )

    group_split_cases = cases_df_sample.partition_by(by=['Level_Type', 'Level'], maintain_order=True)

    start_time = time.time()
    rt_results = pl.concat([calculate_rt(i) for i in group_split_cases])

    rt_runtime = time.time() - start_time
    rt_runtime_avg = rt_runtime / num_levels

    output = {
        'result': rt_results,
        'stats': {'runtime': rt_runtime,
                  'runtime_avg': rt_runtime_avg
                  }
    }
    return output


def load_cases():
    df_raw = pl.read_sql(sql='select * from covid_rt_prep', connection_uri=conn_prod)

    df = (
        df_raw
        .with_columns(
            [
                pl.col('Population_DSHS').cast(pl.UInt32)
            ]
        )
    )

    return df


conn_prod = f'sqlite://db/prod.db'
conn_prod_pandas = sqlite3.connect('db/prod.db')

cleaned_cases = load_cases()
rt_results = get_rt(cleaned_cases)
write_results(rt_results)
