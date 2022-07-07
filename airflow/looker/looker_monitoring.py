from airflow.hooks.base_hook import BaseHook
import pandas as pd

def check_alert_exists(dashboard_id):
    print(f"""\n\nChecking alert element for dashboard {dashboard_id}\n""")

def trigger_to_looker() -> None:
    from airflow.hooks.base_hook import BaseHook
    import looker_sdk
    from looker_sdk.sdk.api40 import models as ml
    from looker.settings import LookerAuthSettings
    import datetime
    from snowflake.connector.pandas_tools import write_pandas


    snf = BaseHook.get_hook('SNOWFLAKE')
    sdk = looker_sdk.init40(config_settings=LookerAuthSettings(my_var="looker"))

    last_statuses = snf.get_pandas_df(f"""
        SELECT DASHBOARD_ID, IS_CORRECT, ARRAY_TO_STRING(ARRAY_AGG(ELEMENTS), '|') ELEMENTS, BOOLAND_AGG(DASHBOARD_CORRECT) DASHBOARD_CORRECT
        FROM (
            SELECT DASHBOARD_ID, IS_CORRECT, ARRAY_TO_STRING(ELEMENTS, '|') ELEMENTS, BOOLAND_AGG(IS_CORRECT) OVER (PARTITION BY DASHBOARD_ID) DASHBOARD_CORRECT
            FROM (
                SELECT DASHBOARD_ID, ELEMENTS, IS_CORRECT
                FROM MONITORING.LOOKER_ALERTS
                WHERE 1=1
                QUALIFY ROW_NUMBER() OVER(PARTITION BY DASHBOARD_ID, MART_NAME ORDER BY RECORD_DTTM DESC)=1) BASE 
            ) BASE 
        GROUP BY 1,2;""").to_dict(orient = 'records')
        
    creators_data = snf.get_pandas_df("SELECT DASHBOARD_ID, ARRAY_TO_STRING(CREATOR, ',') CREATORS FROM MONITORING.LOOKER_DASHBOARDS")
        
    dashboards_ok = []
    [dashboards_ok.append(i['DASHBOARD_ID']) for i in last_statuses if i['DASHBOARD_CORRECT'] and i['DASHBOARD_ID'] not in dashboards_ok]
    dashboards_not_ok = []
    [dashboards_not_ok.append(i['DASHBOARD_ID']) for i in last_statuses if i['DASHBOARD_ID'] not in dashboards_ok and i['DASHBOARD_ID'] not in dashboards_not_ok]
    
    print(f"""\n\n\nFound {str(len(dashboards_ok))} dashboards are now ok!\n\n""")
    for dashboard_id in dashboards_ok:
        snf.run(f"""
            UPDATE MONITORING.LOOKER_ALERTS_CURRENT
            SET ERROR_MSG=NULL 
            WHERE 1=1
                AND DASHBOARD_ID='{dashboard_id}';""")
                
    data = []
    for dashboard_id in dashboards_not_ok:
        elements = []
        l_1 = [i['ELEMENTS'] for i in last_statuses if i['DASHBOARD_ID']==dashboard_id and not i['IS_CORRECT']]
        [elements.extend(i.split('|'))  for i in l_1 if len(i.split('|'))>1]
        elements += [i for i in l_1 if len(i.split('|'))==1]
        if 'ALL' in elements:
            elements = ['ALL']
        else:
            elements = list(set(elements))
        
        creators = creators_data.loc[creators_data['DASHBOARD_ID']==dashboard_id]['CREATORS'].iloc[0]
        error_msg = """Dashboard data may be INCORRECT. Will be fixed in 2-3 business hours (approx.).\nIf you need data earlier please contact maxudachin@xometry.de or {creator_mail}.\nError details: {affected_elements}.
        """.format(creator_mail = creators, affected_elements = ','.join(elements))
        
        source_time = datetime.datetime.now()
        dttm = source_time.strftime('%Y-%m-%d %H:%M:%S')
        
        data += [{'DASHBOARD_ID': dashboard_id, 'ERROR_MSG': error_msg, 'RECORD_DTTM': dttm}]
        check_alert_exists(dashboard_id)
        
    if len(data):
        print(f"""\n\n\nFound {str(len(data))} dashboards with errors!\n\n""")
        df_to_push = pd.DataFrame(data)
     
        snf.run("CREATE OR REPLACE TABLE TEMP.LOOKER_ALERTS_CURRENT LIKE MONITORING.LOOKER_ALERTS_CURRENT;")
        write_pandas(
            conn=snf.get_conn(),
            df=df_to_push,
            table_name='LOOKER_ALERTS_CURRENT',
            schema='TEMP')
        snf.run(f"""
            MERGE INTO MONITORING.LOOKER_ALERTS_CURRENT base 
            USING TEMP.LOOKER_ALERTS_CURRENT temp 
            ON base.DASHBOARD_ID = temp.DASHBOARD_ID
            WHEN MATCHED THEN 
            UPDATE SET 
                base.ERROR_MSG = temp.ERROR_MSG,
                base.RECORD_DTTM = CURRENT_TIMESTAMP::DATETIME
            WHEN NOT MATCHED THEN 
                INSERT (DASHBOARD_ID, ERROR_MSG, RECORD_DTTM)
                VALUES (temp.DASHBOARD_ID, temp.ERROR_MSG, CURRENT_TIMESTAMP::DATETIME);""")
        snf.run("DROP TABLE TEMP.LOOKER_ALERTS_CURRENT;")
        
    snf.run(f"""
            UPDATE MONITORING.LOOKER_ALERTS
            SET IS_INFORMED=TRUE
            WHERE 1=1
                AND NOT IS_INFORMED;""")
    print(f"""\n\n\nAll statuses updated and users informed!\n\n""")

def looker_alerts( is_error, mart_name, error_type) -> None:
    from airflow.hooks.base_hook import BaseHook
    import datetime
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    print(f"""\n\n\nBegan Looker Alerts Check:\n\tMART_NAME:{mart_name};\n\tERROR_TYPE:{error_type};\n\tIS_ERROR:{str(is_error)}\n\n\n""")
    
    snf = BaseHook.get_hook('XOMETRY_EU_SNOWFLAKE')
    
    dashboards_depend= snf.get_pandas_df(f"""
        SELECT 
            DASHBOARD_ID, 
            ARRAY_AGG(ELEMENT_ID)=ARRAY_AGG(CASE WHEN ARRAY_CONTAINS('{mart_name}'::VARIANT, ELEMENT_USES_MARTS) THEN ELEMENT_ID END) IS_ALL,
            LISTAGG(CASE WHEN ARRAY_CONTAINS('{mart_name}'::VARIANT, ELEMENT_USES_MARTS) THEN ELEMENT_TITLE END, '|') ELEMENTS
        FROM MONITORING.LOOKER_ELEMENTS
        WHERE 1=1
            AND REPLACED_AT IS NULL
        GROUP BY 1
        HAVING ARRAY_SIZE(ARRAY_AGG(CASE WHEN ARRAY_CONTAINS('{mart_name}'::VARIANT, ELEMENT_USES_MARTS) THEN COALESCE(ELEMENT_TITLE, ELEMENT_ID) END))>0;""").to_dict(orient = 'records')

    if len(dashboards_depend):
        print(f"""\n\nFound elements depending on this mart!\n\n""")
        
        depend_data = [{'DASHBOARD_ID':i['DASHBOARD_ID'], 'ELEMENTS':['ALL']} for i in dashboards_depend if i['IS_ALL']]+[{'DASHBOARD_ID':i['DASHBOARD_ID'], 'ELEMENTS':i['ELEMENTS'].split("|")} for i in dashboards_depend if not i['IS_ALL']]

        source_time = datetime.datetime.now()
        dttm = source_time.strftime('%Y-%m-%d %H:%M:%S')
        batch = source_time.strftime('%Y%m%d')

        data_to_push = []
        for row in depend_data:
            dict_ = row
            dict_['MART_NAME'] = mart_name
            dict_['IS_CORRECT'] = not is_error
            dict_['IS_INFORMED'] = False
            dict_['ERROR_TYPE'] = error_type
            dict_['ALERT_ELEMENT_ID'] = None
            dict_['RECORD_DTTM'] = dttm
            dict_['BATCH'] = batch
            data_to_push+=[dict_]
        
        df_to_push = pd.DataFrame(data_to_push)
        write_pandas(
            conn=snf.get_conn(),
            df=df_to_push,
            table_name='LOOKER_ALERTS',
            schema='MONITORING')
        print(f"""\n\n--Rows pushed to MONITORING.LOOKER_ALERTS:\t{str(df_to_push.count())}.\n\n""")


    



