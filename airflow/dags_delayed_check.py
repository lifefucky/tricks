from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow import settings
from airflow.models.dagbag import DagBag
from airflow.utils.state import State
from airflow.models.dagrun import DagRun
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

import common.monitoring as mn #local file with slack alerting functions

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 3, 25, 11, 0, 0),
    'retries': 0,
    'retries_delay': timedelta(minutes=15),
    'email': ['mail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': mn.task_fail_slack_alert
    }

dag = DAG(dag_id='dags_delayed_check',
        tags=['sys'],
        default_args=default_args,
        schedule_interval='@hourly' )

def dag_delay_slack_alert(task_instance):
    slack_msg = """
            :turtle: Dag works too long. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=task_instance.task_id,
            dag=task_instance.dag_id,
            ti=task_instance.task_id,
            exec_date=task_instance.execution_date,
            log_url=task_instance.log_url
        )

    delayed_alert = SlackWebhookOperator(
        task_id='delayed_alert',
        http_conn_id='slack_bot',
        message=slack_msg,
        channel='#airflow-alerts')
    return delayed_alert.execute(context=None)


def turtle_dags() -> None:
    db = DagBag(settings.DAGS_FOLDER)

    ri = [i for i in DagRun.find(dag_id=db.dag_ids, state=State.RUNNING) if not db.dags[i.dag_id].get_is_paused()]
    ri_f = [i for i in ri if i.start_date.replace(tzinfo=None) <= (datetime.utcnow() - timedelta(minutes=20))]
    ti = [i.get_task_instances()[-1] for i in ri_f]

    for task in ti:
        dag_delay_slack_alert(task)

    if not ti:
        raise AirflowSkipException('No delayed dags')

turtle_dags = PythonOperator(
    task_id='turtle_dags',
    python_callable=turtle_dags,
    dag=dag)

turtle_dags
