from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

import common.monitoring as mn
from looker.workers import dashboard_build_data
from looker.looker.workers import elements_build_data


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 7, 4, 0, 0, 0),
    'retries': 3,
    'retries_delay': timedelta(minutes=1),
    'email': ['mail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': mn.task_fail_slack_alert}

dag = DAG(
    dag_id='looker_snapshot',
    tags=['sys'],
    default_args=default_args,
    schedule_interval='@daily'
)

'''
Write here parent folders you want to take dashboards elements from
'''
parent_folders = [ 'Folder_Name' ]


scrap_folders = [PythonOperator(
        task_id = f'scrap_folder_{folder.replace(" ","_").lower()}',
        python_callable = dashboard_build_data,
        op_kwargs = {'parent_folder': folder},
        retries = 0,
        retry_delay = timedelta(minutes=5),
        dag = dag) for folder in parent_folders]

push_dashboards = SnowflakeOperator(
    task_id='push_dashboards',
    dag=dag,
    snowflake_conn_id='SNOWFLAKE',
    sql='push_dashboards.sql'
)

scrap_dashboards = PythonOperator(
    task_id='scrap_dashboards',
    python_callable=elements_build_data,
    retries=5,
    retry_delay = timedelta(seconds=30),
    dag=dag
)

push_elements = SnowflakeOperator(
    task_id='push_elements',
    dag=dag,
    snowflake_conn_id='SNOWFLAKE',
    sql='push_elements.sql'
)


scrap_folders >> push_dashboards >> scrap_dashboards >> push_elements