# region imports -----
import time
import pandas as pd
from datetime import datetime as dt
import sqlite3
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest


# endregion

# region functions -----
def run_diagnostics(df):
    context = gx.get_context()
    df_run_date = dt.strftime(df['Date'].max(), '%Y-%m-%d')
    run_name = f'{df_run_date}_PROD_LOAD_COUNTY_TPR'

    tpr_batch_request = RuntimeBatchRequest(
        datasource_name="county_tpr",
        data_connector_name="county_tpr_connector",
        data_asset_name="COUNTY_TPR",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"tpr_identifier": ""},
    )

    result = context.run_checkpoint(
        checkpoint_name="county_tpr_prod_load_check",
        run_name=run_name,
        validations=[
            {"batch_request": tpr_batch_request}
        ],
    )

    context.build_data_docs()
    return result


# searches through raw column names and renames to desired output if substring match found
def clean_column_names(input_df, input_cols):
    column_mapping = {
        'positivity': {'out_col': 'tpr'},
        'Total NAAT': {'out_col': 'tests'},
        'per 100k': {'out_col': 'tests_per_100k'}
    }
    for col_name in input_cols:
        for key, _ in column_mapping.items():
            if key in col_name:
                column_mapping[key].update({'in_col': col_name})

    renamed_df = (
        input_df[input_cols]
        .rename(
            columns={
                column_mapping['positivity']['in_col']: column_mapping['positivity']['out_col'],
                column_mapping['Total NAAT']['in_col']: column_mapping['Total NAAT']['out_col'],
                column_mapping['per 100k']['in_col']: column_mapping['per 100k']['out_col'],
            }
        )
    )

    return renamed_df


def clean_data(raw_file):
    bad_county_values = '|'.join(['unallocated', 'unknown'])
    start_time = time.time()

    col_substrings_to_drop = ['previous', '%', 'latency', 'absolute']
    tpr_cols_raw = raw_file.columns.tolist()
    tpr_cols = [i for i in tpr_cols_raw if not any(x in i.lower() for x in col_substrings_to_drop)]
    renamed_df = clean_column_names(input_df=raw_file, input_cols=tpr_cols)

    final_df = (renamed_df[~renamed_df['County'].str.lower().str.contains(bad_county_values)]
                .assign(County=lambda x: x['County'].str.replace(' County, TX', ''))
                .assign(Date=lambda x: pd.to_datetime(x['Date']))
                )

    final_df = final_df.astype(
        {
            'tpr': 'float32',
            'tests': 'uint32',
            'tests_per_100k': 'float32'
        }
    )

    elapsed_time = time.time() - start_time

    results = {
        'df': final_df,
        'runtime': elapsed_time
    }

    return results


def write_to_prod(df):
    df.to_sql('county_tpr', con=conn_prod, if_exists='append')


def check_diagnostic_results(gx_results):
    checks = None
    return all(checks)

# endregion
conn_stage = sqlite3.connect('db/staging.db')
conn_prod = sqlite3.connect('db/prod.db')

raw_tpr = pd.read_sql("select * from main.tpr_raw", con=conn_stage)
clean_tpr_results = clean_data(raw_tpr)

final_diagnostic_results = run_diagnostics(clean_tpr_results['df'])

if final_diagnostic_results['result'] == 'success':
    write_to_prod(clean_tpr_results[['df']])
else:
    print('ERROR: CHECK DATA DOCS')
