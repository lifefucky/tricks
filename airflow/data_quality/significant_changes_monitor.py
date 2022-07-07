from airflow.hooks.base_hook import BaseHook
from slack_sdk.webhook import WebhookClient
from airflow.exceptions import AirflowSkipException
import pandas as pd
from datetime import datetime

from looker.looker_monitoring import looker_alerts

snf = BaseHook.get_hook('SNOWFLAKE')

def significant_changes_monitor(mart_name):
    print(f'\n\n\nStarted looking for significant changes in {mart_name} after last upload\n\n\n')

    cols_ = snf.get_pandas_df(f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE table_schema='MART_MART_MART' 
        AND table_name='{mart_name}' 
        AND DATA_TYPE IN ('FLOAT', 'NUMBER')
        AND NOT ENDSWITH(COLUMN_NAME, 'ID')
    ORDER BY ordinal_position """)['COLUMN_NAME'].tolist()
    """
    Getting report_dates was uploaded recently and already got data versions from previous uploads, calculate difference in quantity data 
    """
    check_rows = snf.get_pandas_df(f"""
    WITH 
        cte AS (
        select report_date
        from monitoring.{mart_name} 
        group by 1
        having count(*)>1 and MAX(tdate)=CURRENT_DATE)
    SELECT 
        report_date, 
        {','.join(['round(100*sum(diff_{col})/min({col}),2) {col}'.format(col=i) for i in cols_])}
    FROM (
        SELECT 
            tdate, report_date, 
            {','.join(['{col}-lag({col}) OVER (PARTITION BY report_date ORDER BY tdate) diff_{col}, {col}'.format(col=i) for i in cols_])}
        FROM (  SELECT 
                monitoring.*
                FROM monitoring.{mart_name}  monitoring
                JOIN cte ON monitoring.report_date=cte.report_date 
                QUALIFY row_number() OVER (PARTITION BY monitoring.report_date ORDER BY tdate DESC)<3 ) base 
            ) base 
    GROUP BY 1""")

    error = False 

    if check_rows.empty:
        raise AirflowSkipException(f"""\n{mart_name}:No historical changes for today\n""")
    """
    Getting report_dates where we have significant changes in quantity data 
    """

    for col in cols_:
        report_dates = check_rows[(check_rows[col] > 20) | (check_rows[col] < -20)]['REPORT_DATE'].to_list()
        if len(report_dates):
            report_dates = [i.strftime("%Y-%m-%d") for i in report_dates]
            slack_msg = """
                :yellow_circle: Significant changes of quantity data in {mart}
                *Details:* column '{column}' on report_date/-s: {details_dates}.
                *To do:*  Take a look on that using script:
                SELECT *, {column}-lag({column}) OVER (PARTITION BY report_date ORDER BY tdate) diff_{column}
                FROM MONITORING.{mart}
                WHERE REPORT_DATE IN ({where_dates}) 
                ORDER BY REPORT_DATE, TDATE
                Please compare with erp data.                        
                *Hey,* {mention}!""".format(
                mart=mart_name,
                column=col,
                details_dates=','.join(report_dates),
                where_dates=','.join(["'{date}'".format(date=rdate) for rdate in report_dates]),
                mention='@USER')
            webhook = WebhookClient(BaseHook.get_connection('monitoring').host)
            webhook.send(text=slack_msg)
            error = True
        else:
            print(f"""{mart_name}.{col}: No significant changes!""")

    looker_alerts(is_error=error, mart_name=mart_name, error_type = "Significant changes of quantity data")
