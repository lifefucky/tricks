from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeCheckOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from multiprocessing.pool import Pool

import common.monitoring as mn  #local file with slack alerting functions
from data_quality.compare_with_erp import compare_mart_with_erp_changes
from data_quality.significant_changes_monitor import significant_changes_monitor
from data_quality.overall_monitoring import first_monitore

from looker.looker_monitoring import trigger_to_looker

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 4, 18, 0, 0, 0),
    'retries': 0,
    'retries_delay': timedelta(minutes=15),
    'email': ['mail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': mn.task_fail_slack_alert}

dag = DAG(dag_id='data_quality_monitoring',
        tags=['sys'],
        default_args=default_args,
        schedule_interval='00 09 * * *')

snf = BaseHook.get_hook('SNOWFLAKE')

marts = snf.get_pandas_df(f"""
        SELECT MART_NAME
        FROM MONITORING.DATA_MARTS_METRICS
        WHERE SQL_CHECK IS NOT NULL
        """)['MART_NAME'].tolist()
        
def empty_func() -> None:
    pass



overall_monitore = [PythonOperator(
        task_id=f'overall_{mart}',
        python_callable=first_monitore,
        op_kwargs={'mart_name': mart},
        retries=0,
        retry_delay=timedelta(minutes=5),
        dag=dag) for mart in marts]
        
empty_task = PythonOperator(
        task_id='empty_task',
        python_callable=empty_func,
        dag=dag)  
        
compare_mart_erp = [PythonOperator(
        task_id=f'compare_{mart}',
        python_callable=compare_mart_with_erp_changes,
        op_kwargs={'mart_name': mart},
        retries=0,
        retry_delay=timedelta(minutes=5),
        dag=dag) for mart in marts]

empty_task_second = PythonOperator(
        task_id='empty_task_second',
        python_callable=empty_func,
        dag=dag)  
        
changes_mart = [PythonOperator(
        task_id=f'check_changes_{mart}',
        python_callable=significant_changes_monitor,
        op_kwargs={'mart_name': mart},
        retries=0,
        retry_delay=timedelta(minutes=5),
        dag=dag) for mart in marts]

trigger_to_looker = PythonOperator(
        task_id='trigger_to_looker',
        python_callable=trigger_to_looker,
        retries=0,
        dag=dag)  
        

overall_monitore >> empty_task >> compare_mart_erp >> empty_task_second >> changes_mart >> trigger_to_looker