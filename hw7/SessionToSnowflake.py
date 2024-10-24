# In Cloud Composer, add apache-airflow-providers-snowflake to PYPI Packages
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector
import requests
from datetime import datetime, timedelta

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_tables(table_name1, table_name2, cursor):
    try:
        cursor.execute("BEGIN;")
        # Create a table for user information with chanel names and session IDs
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name1} (
            userId int not NULL, sessionId varchar(32) primary key, channel varchar(32) default 'direct'  
        );""")

        # Create another table for the session with timestamps
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name2} (
                sessionId varchar(32) primary key, ts timestamp  
        );""")
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e

@task
def populate_tables(table_name1, table_name2, stage_name, cursor):
    try:
        cursor.execute("BEGIN;")
        # Create an external stage  to use for loading data 
        cursor.execute(f"""CREATE OR REPLACE STAGE {stage_name}
            url = 's3://s3-geospatial/readonly/'
            file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
        """)

        # Load data into target tables
        cursor.execute(f"""COPY INTO {table_name1}
            FROM @{stage_name}/user_session_channel.csv;
        """)
        cursor.execute(f"""COPY INTO {table_name2}
            FROM @{stage_name}/session_timestamp.csv;
        """)
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'Session_To_Snowflake',
    start_date = datetime(2024,10,23),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    user_session_table = "hw6.raw_data.user_session_channel"
    timestamp_table = "hw6.raw_data.session_timestamp"
    stage = "hw6.raw_data.blob_stage"

    cur = return_snowflake_conn()    
    create_tables(user_session_table, timestamp_table, cur) >> populate_tables(user_session_table, timestamp_table, stage, cur)
