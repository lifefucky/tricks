import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup
from dags.sys.looker.looker_monitoring import looker_alerts
from slack_sdk.webhook import WebhookClient


def get_credentials(mart_name):
    '''Receive sql script for comparing with ERP sources and identity keys'''
    from airflow.hooks.base_hook import BaseHook

    import os
    import glob
    from pathlib import Path

    path_to_main = Path(__file__).resolve()
    while True:
        path_to_main = path_to_main.parent
        if os.path.basename(path_to_main) == 'dags':
            break
    path_path = os.path.abspath(path_to_main)

    files_next = [{'FILE': os.path.basename(x), 'PATH': os.path.abspath(x)} for x in
                  glob.glob(path_path + '/mart/*/dq_check_query/*.sql')]

    print(files_next)
    mart_idx = [i['FILE'].upper().replace('.SQL', '') for i in files_next].index(mart_name)
    with open(files_next[mart_idx]['PATH'], 'r') as sql_file:
        sql_query = sql_file.read()

    snf = BaseHook.get_hook('hook')
    fetch_identity_columns = \
    snf.get_pandas_df("SELECT IDS, EXCEPTIONS FROM MONITORING.PRIMARY_KEYS WHERE MART_NAME='{}'".format(mart_name))
    ids = fetch_identity_columns.IDS.iloc[0].split("|")
    exceptions = fetch_identity_columns.EXCEPTIONS.iloc[0]
    add_exceptions =  '' if exceptions is None else ' AND {}'.format(exceptions)

    return {'MART': mart_name, 'IDS': ids, 'SQL': sql_query, 'EXCEPTION': add_exceptions}


def if_any_duplicates(mart_name, id_column, exception):
    '''Check existing duplicates using received identity keys'''
    from airflow.exceptions import AirflowFailException, AirflowSkipException
    from airflow.hooks.base_hook import BaseHook

    from slack_sdk.webhook import WebhookClient

    from dags.sys.looker.looker_monitoring import looker_alerts

    snf = BaseHook.get_hook('snowflake_conn')
    print(f'\n\n\nStarted looking for duplicates in {mart_name} after last upload\n\n\n')

    snf.run(f"""
        CREATE OR REPLACE TABLE TEMP.{mart_name}_duplicates
        AS 
        SELECT {','.join(id_column)} 
        FROM MART.{mart_name} mart
        WHERE 1=1 {exception}
        GROUP BY {','.join(id_column)}
        HAVING COUNT(*)>1""")

    is_any = snf.get_pandas_df(f"""
        SELECT EXISTS(
	    SELECT 1  
        FROM TEMP.{mart_name}_duplicates) IS_EXISTS""").iloc[0].IS_EXISTS

    error = False

    if is_any:
        slack_msg = """
                    :red_circle: Duplicates in table {mart}
                    *Details*: TEMP.{mart}_duplicates
                    Hey, {mention}!
                    """.format(
            mart=mart_name,
            mention='<!subteam^S000000000000>')
        webhook = WebhookClient(BaseHook.get_connection('slack_bot_monitoring').host)
        webhook.send(text=slack_msg)
        error = True
    else:
        snf.run(f"DROP TABLE TEMP.{mart_name}_duplicates")

    looker_alerts(is_error=error, mart_name=mart_name, error_type='Duplicates')

    if error:
        raise AirflowFailException(f"""\nWe need rerun!\n""")
    else:
        raise AirflowSkipException(f"""\nFine!!\n""")


def if_any_unsufficients(mart_name, sql_check, id_column):
    '''Compare mart with ERP using received sql script - if we have old data (<current_date) not existing in mart'''
    from airflow.exceptions import AirflowFailException, AirflowSkipException
    from airflow.hooks.base_hook import BaseHook

    from slack_sdk.webhook import WebhookClient

    from dags.sys.looker.looker_monitoring import looker_alerts

    snf = BaseHook.get_hook('snowflake_conn')
    print(f'\n\n\nStarted looking for unsufficient data in {mart_name} after last upload\n\n\n')
    snf.run(f"""
        CREATE OR REPLACE TABLE TEMP.{mart_name}_unsuff_err
        AS 
        SELECT {','.join(['base.' + i for i in id_column])} 
        FROM ({sql_check}) base
        LEFT JOIN MART.{mart_name} mart ON {' AND '.join(['base.{id_col}=mart.{id_col}'.format(id_col=i) for i in id_column])}
        WHERE mart.{id_column[0]} IS NULL
        AND base.REPORT_DATE<CURRENT_DATE""")

    is_any = snf.get_pandas_df(f"""
        SELECT EXISTS(
	    SELECT 1  
        FROM TEMP.{mart_name}_unsuff_err) IS_EXISTS""").iloc[0].IS_EXISTS

    error = False
    if is_any:
        slack_msg = """
                        :red_circle: Insufficient data in table {mart}
                        *Details*: TEMP.{mart}_unsuff_err
                        Hey, {mention}!
                        """.format(
            mart=mart_name,
            mention='<!subteam^S000000000000>')
        webhook = WebhookClient(BaseHook.get_connection('slack_bot_monitoring').host)
        webhook.send(text=slack_msg)
        error = True
    else:
        snf.run(f"DROP TABLE TEMP.{mart_name}_unsuff_err")

    looker_alerts(is_error=error, mart_name=mart_name, error_type='Unsufficient data')

    if error:
        raise AirflowFailException(f"""\nWe need rerun!\n""")
    else:
        raise AirflowSkipException(f"""\nFine!!\n""")


def easy_quality(dag: DAG, mart_dict) -> TaskGroup:
    '''Data Quality TaskGroup to import in mart dags'''
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup

    easy_taskgroup = TaskGroup(group_id="easy_taskgroup_{}".format(mart_dict['MART']), dag=dag)

    duplicates = PythonOperator(
        task_id="duplicates_{}".format(mart_dict['MART']),
        task_group=easy_taskgroup,
        python_callable=test_if_any_duplicates,
        op_kwargs={'mart_name': mart_dict['MART'], 'id_column': mart_dict['IDS'], 'exception': mart_dict['EXCEPTION']},
        trigger_rule='all_done',
        dag=dag,
    )

    unsufficients = PythonOperator(
        task_id="unsufficients_{}".format(mart_dict['MART']),
        task_group=easy_taskgroup,
        python_callable=test_if_any_unsufficients,
        op_kwargs={'mart_name': mart_dict['MART'], 'sql_check': mart_dict['SQL'], 'id_column': mart_dict['IDS']},
        trigger_rule='all_done',
        dag=dag,
    )

    duplicates >> unsufficients

    return easy_taskgroup
