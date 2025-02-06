from table_schema_creation_addition import *
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from datetime import date, timedelta

args = {
    'owner': 'NAME',
    'retries':10,
    'start_date': datetime.datetime(2022, 10, 22),
    'email':  ['name@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay':  timedelta(minutes = 30)
}

TODAY = date.today()
connection_dwh = gp_connect_to_db(secret_db)
curs_dwh = connection_dwh.cursor()

# Intermediate operation
def check_table_readiness():
    """Check the table if it contains required data to be operated on"""
    try:
        logging.info(f'Checking the max time of table ')
        query_check = f"""
            SELECT
              max(reporting_date)::date as rep_date
            FROM
              table
        """
        df_check = pd.read_sql(query_check, con = connection_dwh)

        if df_check.rep_date[0] != date.today()-timedelta(days=1):
            raise ValueError('Data is not uploaded yet, try again')
        else:
            logging.info("Data is updated")
            pass
    except Exception as e:
        logging.error('Something wrong. It seems that checking for the update is not working', e)
        raise e   
    finally:
        connection_dwh.close() 

# Main operation            
def daily_update_table():
    day = date.today()-timedelta(days=1)
    
    sql_query_dwh= f""" 

    
    select * from table
    
    where reporting_date ='{day}'

    """
    loans = psql.read_sql(sql_query_dwh, connection_dwh)
    
    info  = psql.read_sql(f"""
        select * from other_table
        """, connection_dwh)
    
    #DO SOME OPERATIONS ON BOTH TABLES
    merged_df = pd.merge(loans, info, how= 'left', on = 'column')
    logging.info(len(merged_df))
    merged_df['days_difference'] = merged_df.apply(lambda x: (pd.to_datetime(x.reporting_date) - pd.to_datetime(x.selected_date)).days if str(x.okay_column)!=False else None, axis =1)
    logging.info(merged_df)
   
    required_df  = psql.read_sql(f"""select * from third_table limit 1""", connection_dwh)
    list_needed = required_df.columns.to_list()
    "REINDEX TABLE"
    merged_df = merged_df.reindex(columns = list_needed)
    
    add_table_psql(merged_df,'table_name','schema_name',connection_dwh,curs_dwh)
    
    logging.info('finished inserting',len(merged_df))


# Main run
with DAG('dag_name', description='dag_description', schedule_interval='00 11 * * *', catchup=False, default_args = args) as dag:
        check_table_readiness = PythonOperator(task_id='check_table_readiness', python_callable=check_table_readiness)
        
        daily_update_table = PythonOperator(task_id='daily_update_table', python_callable = daily_update_table)
        check_table_readiness>>daily_update_table