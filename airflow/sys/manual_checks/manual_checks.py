from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
import os
import glob
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeCheckOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 4, 15, 0, 0, 0),
    'retries': 0,
    'retries_delay': timedelta(minutes=5)
    }

path_to_folder = os.path.dirname(os.path.realpath(__file__))

files = [{ "file":os.path.basename(x), "path": os.path.abspath(x).replace(path_to_folder, '') } 
            for x in glob.glob(path_to_folder+'/queries/*.sql')]

dag = DAG(dag_id='manual_checks',
          tags=['monitoring_dwh', 'sys'],
          default_args=default_args,
          schedule_interval=timedelta(hours=1))

[
    SnowflakeCheckOperator(
    task_id=file['file'],
    dag=dag,
    snowflake_conn_id='snowflake_conn_id',
    sql=file['path']) 
    for file in files
]