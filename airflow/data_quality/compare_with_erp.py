from airflow.hooks.base_hook import BaseHook
from slack_sdk.webhook import WebhookClient
from airflow.exceptions import AirflowSkipException
import pandas as pd
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas

from looker.looker_monitoring import looker_alerts

snf = BaseHook.get_hook('SNOWFLAKE')

def compare_mart_with_erp_changes(mart_name):
        print(f'\n\n\nStarted comparing data uploaded to {mart_name} with erp data\n\n\n')
                
        mart_metrics = snf.get_pandas_df(f"""
        SELECT SQL_CHECK, ID_COLUMN
        FROM MONITORING.DATA_MARTS_METRICS
        WHERE MART_NAME='{mart_name}'""")
        sql_check = mart_metrics['SQL_CHECK'].tolist()[0]
        id_columns = [i for i in mart_metrics['ID_COLUMN'].tolist()[0].split("|")]
        
        """
        Get last changes from mart table, using monitoring table (look at airflow-dags/dags/common/success_monitoring.py)
        """
        monitor_data = snf.get_pandas_df(f"""
        SELECT * 
        FROM MONITORING.{mart_name}
        WHERE REPORT_DATE<CURRENT_DATE
        QUALIFY TDATE=MAX(TDATE) OVER (PARTITION BY 1)""")
        report_dates = monitor_data['REPORT_DATE'].tolist()
        report_dates = [i.strftime("%Y-%m-%d") for i in report_dates]

        get_cols = snf.get_pandas_df(f"""SELECT * FROM ({sql_check}) base LIMIT 1""").columns
        """
        Get common columns of erp check script and monitoring table for compare
        """
        all_cols = [i for i in monitor_data.columns if i in get_cols]
        cols = [col for col in all_cols if col!='REPORT_DATE']
        if not len(cols):
            raise AirflowSkipException(f"""\n{mart_name}: No columns to compare raw erp script with mart.\n""")
        """
        Get erp data from sql_check script on dates where we have changes
        """
        erp_data = snf.get_pandas_df(f"""
        SELECT {','.join(['REPORT_DATE::DATE REPORT_DATE']+['SUM({column}) {column}'.format(column=col) for col in cols])}
        FROM ({sql_check}) base 
        WHERE report_date IN ({','.join(["'{date}'".format(date=rdate) for rdate in report_dates])})
        AND report_date<CURRENT_DATE
        GROUP BY report_date """)
        monitor_data = monitor_data[all_cols]
        """
        Drop rows where we have the same values between erp and mart
        """
        compare = pd.concat([erp_data, monitor_data]).drop_duplicates(subset=all_cols, keep=False)

        error = False
        if compare.shape[0]:
            dismatch_dt = compare['REPORT_DATE'].to_frame().drop_duplicates(subset='REPORT_DATE')
            dismatch_dt['MART_NAME'] = mart_name
            snf.run(f"""DELETE FROM MONITORING.DATA_COMPARE WHERE MART_NAME='{mart_name}'""")
            write_pandas(
                    conn=snf.get_conn(), 
                    df=dismatch_dt, 
                    table_name = 'DATA_COMPARE', 
                    schema = 'MONITORING')
               
            sql_query = f"""
CREATE OR REPLACE TABLE TEMP.{mart_name}_COMPARE_WITH_ERP
AS
SELECT 	
{','.join(['COALESCE(erp.{id}, mart.{id}) {id}'.format(id=i) for i in id_columns])},
MAX(erp.REPORT_DATE) erp_REPORT_DATE,  
MAX(mart.REPORT_DATE) mart_REPORT_DATE,
{','.join(['SUM(erp.{column}) erp_{column}, SUM(mart.{column}) mart_{column}'.format(column=col) for col in cols])}
FROM ({sql_check}) ERP 
FULL JOIN MART.{mart_name} mart ON {' AND '.join(['erp.{id}=mart.{id}'.format(id=i) for i in id_columns])}
JOIN MONITORING.DATA_COMPARE compare ON compare.REPORT_DATE=COALESCE(ERP.REPORT_DATE, mart.report_date) and compare.MART_NAME='{mart_name}'
GROUP BY {','.join(['COALESCE(erp.{id}, mart.{id})'.format(id=i) for i in id_columns])}
HAVING MAX(erp.REPORT_DATE)!=MAX(mart.REPORT_DATE) OR {' OR '.join(['to_number(SUM(erp.{column}), 38, 3)!=to_number(SUM(mart.{column}), 38, 3)'.format(column=col) for col in cols])};"""
            print(f"""\nFill the table with dismatched values using script:\n\n{sql_query}\n\n""")
            snf.run(sql_query)

            is_any = snf.get_pandas_df(f"""
            SELECT EXISTS(
            SELECT 1  
            FROM TEMP.{mart_name}_COMPARE_WITH_ERP) IS_EXISTS""").iloc[0].IS_EXISTS

            if is_any: 
            
                               
                slack_msg = """
                :red_circle: Dismatch of quantity values' sums in {mart}
                *Details:* See details in table TEMP.{mart}_COMPARE_WITH_ERP
                *To do:* After fixing issues, please rerun mart's dag and check data quantity one more time.                          
                *Hey,* {mention}!""".format(
                mart=mart_name,
                mention = '@User')
                webhook = WebhookClient(BaseHook.get_connection('monitoring').host)
                webhook.send(text=slack_msg)

                error=True

            else:
                raise AirflowSkipException(f"""\n{mart_name}: No difference found! Good job!\n""")
        else:
            raise AirflowSkipException(f"""\n{mart_name}: No difference found! Good job!\n""")

        looker_alerts(is_error=error, mart_name=mart_name, error_type = "Dismatch of quantity values' sums with ERP")


