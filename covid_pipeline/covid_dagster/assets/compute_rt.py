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
def rt_results(context, rt_prep_df: pl.DataFrame) -> pl.DataFrame:
    def get_case_timeseries(cases: pl.Series, rt_dates: pl.Series) -> pd.Series:
        return cases.to_pandas().set_axis(rt_dates.to_pandas())

    def calculate_rt(df: pl.DataFrame) -> pl.DataFrame:
        print(df.get_column('level')[0])

        case_timeseries = get_case_timeseries(df.get_column('cases_ma_7day'), df.get_column('date'))
        assert case_timeseries.index.is_unique
        assert type(case_timeseries.index) == pd.DatetimeIndex

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

    def get_rt(cleaned_cases: pl.DataFrame) -> dict:
        sample_levels = ['Harris', 'Bexar', 'Travis']
        num_levels = len(sample_levels)

        cases_df_sample = (
            cleaned_cases
            .filter(pl.col('level').is_in(sample_levels))
        )

        group_split_cases = cases_df_sample.partition_by(by=['level_type', 'level'], maintain_order=True)

        start_time = time.time()
        # TODO: implement parallel processing
        rt_result = pl.concat([calculate_rt(i) for i in group_split_cases])

        rt_runtime = time.time() - start_time
        rt_runtime_avg = rt_runtime / num_levels

        output = {
            'result': rt_result,
            'stats': {'runtime': rt_runtime,
                      'runtime_avg': rt_runtime_avg
                      }
        }
        return output

    # convert date to string, then to datetime
    rt_prep_df_formatted = (
        rt_prep_df
        .with_columns(
            rt_prep_df['date']
            .dt
            .strftime('%Y-%m-%d')
            .str
            .to_datetime('%Y-%m-%d')
            .alias('date')
        )
    )
    rt_output = get_rt(rt_prep_df_formatted)
    # TODO: update with correct syntax
    #  add stats to metadata context for logging in dbt
    # context.add_metadata(rt_output['stats'])
    return rt_output['result']

# testing
# schema = 'intermediate'
# table_name = 'rt_prep_df'
#
# from dotenv import load_dotenv
#
# load_dotenv()
#
# from dagster import build_output_context, build_input_context
#
# context = build_input_context(
#     upstream_output=build_output_context(name="def", step_key="123",
#                                          metadata={'schema': 'origin', 'table_name': 'test'})
# )
#
# conn = os.getenv('LOCAL_CONN')
# query = f'select * from {schema}.{table_name}'
# rt_prep_df = pl.read_database(query=query, connection_uri=conn)
# test_result = rt_results(context, rt_prep_df)
