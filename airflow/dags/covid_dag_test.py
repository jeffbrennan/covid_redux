from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime as dt

def get_vitals(url, file_name):
    from datetime import datetime as dt
    import pandas as pd
    from datetime import date

    sheetnames = pd.ExcelFile(url).sheet_names
    new_sheet_name = sheetnames[-1]
    current_year = str(dt.today().year)
    assert current_year in new_sheet_name

    raw_df = pd.read_excel(url, sheet_name=new_sheet_name, skiprows=2)
    df_cols = raw_df.columns.to_list()
    df_cols_parsed = [dt.strftime(i, '%m/%d/%Y') if isinstance(i, date) else i for i in df_cols]
    raw_df.columns = df_cols_parsed

    raw_df.to_parquet(f'./data_dump/{file_name}.parquet')


def write_db(df_name, schema_name, table_name, upload_type='replace'):
    import pandas as pd
    from sqlalchemy import create_engine
    conn = create_engine(Variable.get('conn_string'))

    df = pd.read_parquet(f'./data_dump/{df_name}.parquet')
    df.to_sql(
        name=table_name,
        con=conn,
        schema=schema_name,
        if_exists=upload_type,
        index=False
    )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'catchup': False,
    'start_date': dt(2023, 3, 23),
    'email': ['jeffbrennan10@gmail.com'],
    'retries': 0,
}

with DAG(
        dag_id='covid_dag',
        schedule_interval='@weekly',
        default_args=default_args
) as dag:
    get_case_task = PythonOperator(
        task_id='get_cases',
        python_callable=get_vitals,
        op_kwargs={
            'url': Variable.get('dshs_case_url'),
            'file_name': 'raw_cases_daily'}
    )

    write_db_cases = PythonOperator(
        task_id='write_db_cases',
        python_callable=write_db,
        op_kwargs={
            'df_name': 'raw_cases_daily',
            'schema_name': 'staging',
            'table_name': 'county_vitals_cases'
        }
    )

    get_case_task >> write_db_cases
