from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

def task_fail_slack_alert(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url
        )
    failed_alert = SlackWebhookOperator(
        task_id='failed_alert',
        http_conn_id='slack_bot',
        message=slack_msg,
        channel='#airflow-alerts')
    return failed_alert.execute(context=context)