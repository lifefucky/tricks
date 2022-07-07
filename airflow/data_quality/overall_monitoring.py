from airflow.hooks.base_hook import BaseHook
import pandas as pd
from slack_sdk.webhook import WebhookClient
import datetime
from looker.looker_monitoring import looker_alerts


snf =  BaseHook.get_hook('SNOWFLAKE')

def if_any_duplicates(mart_name, id_column):
    print(f'\n\n\nStarted looking for duplicates in {mart_name} after last upload\n\n\n')
    
    snf.run(f"""
        CREATE OR REPLACE TABLE TEMP.{mart_name}_duplicates
        AS 
        SELECT {','.join(id_column)} 
        FROM MART.{mart_name} mart
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
                        mention='@User')
        webhook = WebhookClient(BaseHook.get_connection('monitoring').host)
        webhook.send(text=slack_msg)
        error =  True
    else:
        snf.run(f"DROP TABLE TEMP.{mart_name}_duplicates")

    looker_alerts(is_error=error, mart_name=mart_name, error_type='Duplicates')
            
def if_any_unsufficients(mart_name, sql_check, id_column):
    print(f'\n\n\nStarted looking for unsufficient data in {mart_name} after last upload\n\n\n')
    snf.run(f"""
        CREATE OR REPLACE TABLE TEMP.{mart_name}_unsuff_err
        AS 
        SELECT {','.join(['base.'+i for i in id_column])} 
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
                        mention='@user')
        webhook = WebhookClient(BaseHook.get_connection('monitoring').host)
        webhook.send(text=slack_msg)
        error = True
    else:
        snf.run(f"DROP TABLE TEMP.{mart_name}_unsuff_err")

    looker_alerts(is_error=error, mart_name=mart_name, error_type = 'Unsufficient data')


def log_changes_metadata(mart_name):
    print(f'\n\n\nWriting sums of quantity values from {mart_name} to monitoring log table \n\n\n')
    t1 =  datetime.datetime.now().replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
    snf.run(f"""
        CREATE OR REPLACE TABLE TEMP.{mart_name}_CHECK
        AS
        SELECT report_date
        FROM (
        SELECT * 
        FROM (
            SELECT 
	            *
            FROM mart.{mart_name} 
                changes(information => default)
                at(timestamp => '{t1}'::timestamp) ) BASE 
        WHERE 1=1 )base
        GROUP BY 1""")

    mart_cols  =  snf.get_pandas_df(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema='MART' 
        AND table_name='{mart_name}' 
        AND DATA_TYPE IN ('FLOAT', 'NUMBER')
        AND NOT ENDSWITH(COLUMN_NAME, 'ID')
        ORDER BY ordinal_position """)['COLUMN_NAME'].tolist()
        
    monitor_cols = snf.get_pandas_df(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_schema='MONITORING' 
        AND table_name='{mart_name}' 
        AND DATA_TYPE IN ('FLOAT', 'NUMBER')
        AND NOT ENDSWITH(COLUMN_NAME, 'ID')
        ORDER BY ordinal_position """)['COLUMN_NAME'].tolist()
        
    diff =  [col for col in mart_cols if not col in monitor_cols]
    if len(diff):
        slack_msg = """
                        :yellow_circle: Please add column/-s {columns} to MONITORING.{mart}
                        Hey, {mention}!
                        """.format(
                            columns=','.join(["'{col}'".format(col=i) for i in diff]),
                            mart=mart_name,
                            mention='@User')
        webhook = WebhookClient(BaseHook.get_connection('smonitoring').host)
        webhook.send(text=slack_msg)
            
    snf.run(f"""
            INSERT INTO MONITORING.{mart_name}
            SELECT CURRENT_DATE tdate, mart.report_date, {','.join(['SUM({col}) {col}'.format(col=i) for i in monitor_cols])}
            FROM MART.{mart_name} mart
            JOIN TEMP.{mart_name}_CHECK check_ ON mart.report_date=check_.report_date  
            GROUP BY 2""") 
    snf.run(f"""DROP TABLE TEMP.{mart_name}_CHECK;""")
    snf.run(f"""
        DELETE FROM monitoring.{mart_name} monitoring
        USING (
        SELECT tdate, report_date
        FROM monitoring.{mart_name}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY REPORT_DATE, {','.join(monitor_cols)} ORDER BY tdate)>1) cte  
        WHERE monitoring.tdate=cte.tdate AND monitoring.report_date=cte.report_date ;""")

def first_monitore(mart_name):
    mart_metrics = snf.get_pandas_df(f"""
        SELECT MART_NAME, SQL_CHECK, ID_COLUMN 
        FROM MONITORING.DATA_MARTS_METRICS
        WHERE SQL_CHECK IS NOT NULL
        AND MART_NAME='{mart_name}'""")
    mart_name_ = mart_metrics['MART_NAME'].tolist()[0]
    sql_check_ = mart_metrics['SQL_CHECK'].tolist()[0]
    id_column_ = [i for i in mart_metrics['ID_COLUMN'].tolist()[0].split("|")]
        
    if_any_duplicates(mart_name_, id_column_)
    if_any_unsufficients(mart_name_, sql_check_, id_column_)
    log_changes_metadata(mart_name_)












