from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime as dt
from tasks.extract.get_covid_vitals_daily import get_vitals, write_db
from tasks.transform.clean_county_vitals import clean_staging_vitals, run_diagnostics, write_vitals_to_prod

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

    get_death_task = PythonOperator(
        task_id='get_deaths',
        python_callable=get_vitals,
        op_kwargs={
            'url': Variable.get('dshs_death_url'),
            'file_name': 'raw_deaths_daily'
        }
    )

    write_db_cases = PythonOperator(
        task_id='write_db_cases',
        python_callable=write_db,
        op_kwargs={'df_name': 'raw_cases_daily', 'table_name': 'county_vitals_cases'}
    )

    write_db_deaths = PythonOperator(
        task_id='write_db_deaths',
        python_callable=write_db,
        op_kwargs={'df_name': 'raw_deaths_daily', 'table_name': 'county_vitals_deaths'}
    )

    clean_staging_vitals = PythonOperator(
        task_id='clean_staging_vitals',
        python_callable=clean_staging_vitals,
    )

    run_diagnostics = PythonOperator(
        task_id='run_diagnostics',
        python_callable=run_diagnostics,
    )

    write_vitals_to_prod = PythonOperator(
        task_id='write_vitals_to_prod',
        python_callable=write_vitals_to_prod,
    )

    # [get_case_task, get_death_task] >> write_db_cases >> write_db_deaths >> clean_staging_vitals >> \
    run_diagnostics >> write_vitals_to_prod
