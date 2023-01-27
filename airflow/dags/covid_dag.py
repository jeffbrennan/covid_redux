#region Imports -----


#region airflow -----

from airflow import DAG
from random import randint

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
#endregion

#region general -----
import pandas as pd
import re
from datetime import datetime as dt
from datetime import timedelta


import os

#endregion

#endregion

#region globals -----
if dt.utcnow().hour > 4 and dt.utcnow().hour < 16:
    TODAY = dt.now() - timedelta(days=1)
else:
    TODAY = dt.now()


os.chdir('C:/users/jeffb/Desktop/Life/personal-projects/de/covid_redux')

with DAG(
        "my_dag",
        start_date=dt.datetime(2021,1,1),
        schedule_interval="@daily",
        catchup=False
) as dag:
    # run 3 models that select number between 1 and 10
    get_county_data = PythonOperator(
        task_id="get_county_vitals",
        python_callable=get_county_data  #calls python function _training_model()
    )

    get_county_vitals = PythonOperator(
        task_id="get_county_vitals",
        python_callable=get_county_vitals  # calls python function _training_model()
    )

    write_db = PythonOperator(
        task_id="upload_county_vitals",
        python_callable=write_db  # calls python function _training_model()
    )

   #  run_statistics = PythonOperator(
   #      task_id="run_statistics",
   #      python_callable=_training_model  # calls python function _training_model()
   #  )
   #
   #  upload_statistics = PythonOperator(
   #      task_id="run_statistics",
   #      python_callable=_training_model  # calls python function _training_model()
   # )



   [get_county_data] >> [get_county_vitals] >> [write_db]
   # >> [run_statistics] >> [upload_statistics]