import time
import polars as pl
import epyestim.covid19 as covid19
from dagster import asset, AssetIn
import pandas as pd


@asset(
    name='rt_results',
    group_name="intermediate",
    key_prefix=['intermediate'],
    ins={'rt_prep_df': AssetIn(key=['covid', 'intermediate', 'rt_prep_df'])},
    metadata={
        'schema': 'intermediate',
        "table_name": 'rt_results'
    },
    io_manager_key='polars_io_manager'
)
def rt_results(rt_prep_df: pl.DataFrame) -> pl.DataFrame:
    def get_case_timeseries(cases: pl.Series, dates: pl.Series) -> pd.Series:
        return cases.to_pandas().set_axis(dates.to_pandas())

    def calculate_rt(df: pl.DataFrame) -> pl.DataFrame:
        print(df.get_column('level')[0])

        case_timeseries = get_case_timeseries(df.get_column('Cases_MA_7day'), df.get_column('Date'))
        rt_results_raw = covid19.r_covid(case_timeseries)

        rt_results_clean = (
            pl.from_pandas(rt_results_raw.reset_index())
            .rename({'index': 'date',
                     'R_mean': 'rt',
                     'Q0.025': 'ci_low',
                     'Q0.975': 'ci_high'
                     }
                    )
            [['date', 'rt', 'ci_low', 'ci_high']]
            .with_columns(
                pl.col('date').dt.date().alias('date')
            )
        )

        final_result = (
            df
            .with_columns(
                pl.col('date').dt.date().alias('date')
            )
            .join(other=rt_results_clean, on='date', how='left')
        )

        return final_result

    def get_rt(cleaned_cases: pl.DataFrame) -> pl.DataFrame:
        sample_levels = ['Harris', 'Bexar', 'Travis']
        num_levels = len(sample_levels)

        cases_df_sample = (
            cleaned_cases
            .filter(pl.col('level').is_in(sample_levels))
        )

        group_split_cases = cases_df_sample.partition_by(by=['level_type', 'level'], maintain_order=True)

        start_time = time.time()
        rt_result = pl.concat([calculate_rt(i) for i in group_split_cases])

        rt_runtime = time.time() - start_time
        rt_runtime_avg = rt_runtime / num_levels

        # output = {
        #     'result': rt_results,
        #     'stats': {'runtime': rt_runtime,
        #               'runtime_avg': rt_runtime_avg
        #               }
        # }
        return rt_result

    # rt_result = get_rt(rt_prep_df)
    rt_result = (
        rt_prep_df
        .filter(pl.col('level').is_in(['Harris', 'Bexar', 'Travis']))
        .with_columns(last_updated=pl.lit(time.time_ns()))

    )
    return rt_result
