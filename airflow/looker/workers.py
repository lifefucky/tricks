from airflow.exceptions import AirflowSkipException
from airflow.hooks.base_hook import BaseHook
from dags.sys.looker.settings import LookerAuthSettings
import looker_sdk 
from looker_sdk import models, error
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

def dashboard_build_data(parent_folder) -> None:
    print(f"""\n\n\nStart to parse folder:'{parent_folder}'\n\n""")
    snf = BaseHook.get_hook('SNOWFLAKE')
    sdk = looker_sdk.init40(config_settings=LookerAuthSettings(my_var="looker"))
    
    parent_folder_ids = []
    parent_folder_found = sdk.search_folders(name=parent_folder)
    if len(parent_folder_found)==1:
        parent_folder_ids = [folder.id for folder in parent_folder_found]
    elif len(parent_folder_found)==0:
        raise AirflowSkipException(f"""\n{parent_folder}: No dashboards' folders found with this name!\n""")
    else:
        raise AirflowSkipException(f"""\n{parent_folder}: Too many folders found with this name!\n""")
    
    creators_data = snf.get_pandas_df("SELECT * FROM MONITORING.LOOKER_CREATORS")
    
    dashboards = []
    for folder_id in parent_folder_ids:
        folder = sdk.search_folders(id=folder_id)
        if len(folder)==1:
            folder = folder[0]
            for dashboard in folder.dashboards:
                dashboard_id = dashboard.id
                dashboard_name = dashboard.title 
                creators = [creators_data.loc[creators_data['CREATOR']==creator.display_name]['EMAIL'].iloc[0] for creator in sdk.search_users(id = dashboard.user_id)]
                if not len(creators):
                    creators = [creator.display_name for creator in sdk.search_users(id = dashboard.user_id)]
                
                dashboards += [{'DASHBOARD_ID':dashboard_id, 'DASHBOARD_NAME': dashboard_name, 'CREATOR': creators}]
            child_folders = [child_folder.id for child_folder in sdk.search_folders(parent_id=folder.id)]
            if len(child_folders):
                parent_folder_ids += child_folders

    df = pd.DataFrame(dashboards)
    snf.run("""CREATE OR REPLACE TABLE TEMP.DASHBOARD_DATA(dashboard_id VARCHAR(100),dashboard_name VARCHAR(1000), creator ARRAY);""")
    #snf = BaseHook.get_hook('XOMETRY_EU_SNOWFLAKE')
    write_pandas(snf.get_conn(),
                 df,
                 table_name='DASHBOARD_DATA',
                 schema='TEMP')

def elements_build_data() -> None:
    import time
    
    snf = BaseHook.get_hook('SNOWFLAKE')
    sdk = looker_sdk.init40(config_settings=LookerAuthSettings(my_var="looker"))

    datamarts = snf.get_pandas_df(f"""
        SELECT MART_NAME
        FROM MONITORING.DATA_MARTS_METRICS
        WHERE SQL_CHECK IS NOT NULL
        """)['MART_NAME'].tolist()

    dashboards = snf.get_pandas_df(f"""
        SELECT DASHBOARD_ID
        FROM MONITORING.LOOKER_DASHBOARDS
        WHERE REPLACED_AT IS NULL
        """)['DASHBOARD_ID'].tolist()
        
    exist_elements =  snf.get_pandas_df("""
    select element_id 
    from temp.element_data
    group by 1 
    """)['ELEMENT_ID'].tolist()
    
    '''snf.run("""CREATE OR REPLACE TABLE TEMP.ELEMENT_DATA(
    dashboard_id VARCHAR(100),
    element_id VARCHAR(1000),
    element_title VARCHAR(1000),
    element_model ARRAY,
    element_view ARRAY,
    element_uses_marts ARRAY);""")'''
    
    elements = []
    for id in dashboards:
        dashboard = sdk.search_dashboards(id=id)[0]
        print(f"""\n\nParsing dashboard number {str(id)}\n""")
        dashboard_elements = dashboard.dashboard_elements
        for element in dashboard_elements:
            '''if not len(elements) % 50 and len(elements)>1:
                time.sleep(15)
                sdk = looker_sdk.init40(config_settings=LookerAuthSettings(my_var="looker"))
                
                df = pd.DataFrame(elements)
                write_pandas(snf.get_conn(),
                    df,
                    table_name='ELEMENT_DATA',
                    schema='TEMP')
                elements = []'''
            print(f"""\n\n\tSearching for data of element number {element.id}\n""")
            element_id = element.id
            if element_id in exist_elements:
                continue
            element_title = element.title
            element_model = []
            element_view = []
            if not element.look is None:
                element_model += [element.look.query.model]
                element_view += [element.look.query.view]
            elif not element.query is None:
                element_model += [element.query.model]
                element_view += [element.query.view]
            elif not element.result_maker is None and not element.result_maker.filterables is None:
                element_model += [filterable.model for filterable in element.result_maker.filterables]
                element_view += [filterable.view for filterable in element.result_maker.filterables]
            
            query = ''
            if 'target_model where mart views placed' in element_model:
                try:
                    if not element.query_id is None:
                        query = sdk.run_query(element.query_id, result_format="sql")
                    elif not element.look is None:
                        query = sdk.run_query(element.look.query.id, result_format="sql")
                except error.SDKError:
                    query = 'Error'
            uses_marts = [mart for mart in datamarts if mart in query.upper()]

            elements += [{'DASHBOARD_ID': id, 'ELEMENT_ID': element_id, 'ELEMENT_TITLE': element_title, 'ELEMENT_MODEL': element_model, 'ELEMENT_VIEW': element_view, 'ELEMENT_USES_MARTS': uses_marts}]

    df = pd.DataFrame(elements)
    write_pandas(snf.get_conn(),
                df,
                table_name='ELEMENT_DATA',
                schema='TEMP')
    print("\n\nAll elements has been updated!")



