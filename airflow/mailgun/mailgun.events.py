from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from snowflake.connector.pandas_tools import write_pandas


import requests
import json
from datetime import datetime, timedelta
from email import utils
import pandas as pd

mailgun_connection = BaseHook.get_connection("MAILGUN")

API_KEY = mailgun_connection.password
API_HOST = mailgun_connection.host
DOMAIN_NAME = mailgun_connection.login
LIMIT_PER_PAGE = 300

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 6, 17, 0, 0, 0),
    'retries': 1,
    'retries_delay': timedelta(minutes=15),
    'email': ['mail@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': mn.task_fail_slack_alert}

# create or replace table "XOMETRY_DATABASE"."MAILGUN"."EVENTS" (
#    ID          VARCHAR    NOT NULL
#   ,"EVENT"     VARCHAR
#   ,RECIPIENT   VARCHAR
#   ,"URL"       VARCHAR
#   ,IP          VARCHAR
#   ,TIMESTAMP   TIMESTAMP_NTZ(6)    NOT NULL
#   ,ENVELOPE    VARCHAR
#   ,MESSAGE     VARCHAR
# )

def parse_json(json_string):
    iterable = parse(json_string, [])
    dict_ = {}
    for row in iterable:
        key_ = [*row][0]
        dict_[key_]=row.get(key_)
    return dict_

def parse(json_file, simple_list):
    for key, value in json_file.items():
        dict_ = {}
        dict_[key] = value
        simple_list += [dict_]

        if isinstance(value, dict):
            simple_list+=parse(value, [])
    return simple_list

def case_one_empty(dict_source, expr_list):
    common_list = [dict_source.get(expr, {}) for expr in expr_list]
    str_list = [i for i in common_list if isinstance(i, str)]
    list_list = ['|'.join(i) for i in common_list if isinstance(i, list)]
    return '|'.join(str_list+list_list)

def xstr(s):
    return '' if s is None else str(s)


def get_log_items(begin_date):
    print(f"Fetch events from {begin_date}")

    response = requests.get(
        f"{API_HOST}/v3/{DOMAIN_NAME}/events",
        auth=("api", API_KEY),
        params={
            "begin": begin_date,
            "ascending": 'yes',
            "limit": LIMIT_PER_PAGE,
        })

    if response.status_code != 200:
        raise Exception("Oops! Something went wrong: %s" % response.content)

    json_items = response.json()['items']

    if not json_items:
        print('Items is empty!')
        return []
   
    
    print(f"Fetched {len(json_items)} items")

    if (len(json_items) == LIMIT_PER_PAGE):
        last_datetime_object = datetime.utcfromtimestamp(
            json_items[-1]['timestamp'])
        rfc_date = utils.format_datetime(last_datetime_object)
        json_items = json_items + get_log_items(rfc_date)

    return json_items


def log_items_to_dataframe(json_items):
    headers = [
        'ID',
        'EVENT',
        'RECIPIENT',
        'URL',
        'IP',
        'TIMESTAMP',
        'ENVELOPE',
        'MESSAGE',
    ]
    rows = []

    for item in json_items:
        
        item_data = parse_json(item)
        timestamp_datetime_object = datetime.utcfromtimestamp(
            item_data['timestamp'])
        rfc_timestamp = utils.format_datetime(timestamp_datetime_object)
        cols = [
            item_data['id'],
            item_data['event'],
            case_one_empty(item_data, ['recipient', 'recipients']),
            item_data.get('url'),
            item_data.get('ip'),
            rfc_timestamp,
            json.dumps(item_data.get('envelope', {})),
            json.dumps(item_data.get('message', {})),
            ]
           
        rows.append(cols)

    print(f"Fetched {len(rows)} total rows")

    df = pd.DataFrame(rows, columns=headers)

    return df


def fetch_logs_to_dataframe(rfc_begin_date):
    log_items = get_log_items(rfc_begin_date)

    return log_items_to_dataframe(log_items)

def update_mailgun_to_snowflake():
    target_table = 'MAILGUN.EVENTS_NOW'
    temp_table = 'TEMP.MAILGUN_EVENTS'

    print("Get snowflake connection")
    snf = BaseHook.get_hook('XOMETRY_EU_SNOWFLAKE')

    last_update = snf.get_first(f"select max(tt.timestamp) from {target_table} tt")[0]
    print('Latest update from ', str(last_update))
    datetime_object = datetime.strptime(str(last_update), '%Y-%m-%d %H:%M:%S')
    rfc_begin_date = utils.format_datetime(datetime_object)
    print("Get source data")
    source_mailgun = fetch_logs_to_dataframe(rfc_begin_date)

    print("Drop temp table")
    snf.run(f'DROP TABLE IF EXISTS {temp_table}')

    print("Create temp table from source")
    snf.run(f'CREATE TABLE {temp_table} LIKE {target_table}')

    table_name = temp_table.split('.')[1].replace('"',"")
    schema = temp_table.split('.')[0].replace('"',"")
    print(f'Try write data into snowflake: {table_name} {schema}')
    write_pandas(snf.get_conn(), 
                source_mailgun, 
                table_name=table_name, 
                schema=schema, 
                quote_identifiers = True)

    print("Merge data from temp table to target")
    snf.run(f"""
        BEGIN TRANSACTION;
        DELETE FROM {target_table} w USING {temp_table} s WHERE w.id = s.id;
        INSERT INTO {target_table} SELECT * FROM {temp_table} s;
        COMMIT;""")

# update_mailgun_to_snowflake()

dag = DAG(dag_id='mailgun_shema_update',
        tags=['mailgun'],
        default_args=default_args,
        schedule_interval=timedelta(minutes=30)) 


t1 = PythonOperator(
    task_id='mailgun_events_load',
    python_callable=update_mailgun_to_snowflake,
    op_kwargs={'table': 'events'},
    retries=1,
    retry_delay=timedelta(minutes=5),
    dag=dag)

t1