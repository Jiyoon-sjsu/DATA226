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
def create_summary(table_name1, table_name2, destination, cursor):
    try:
        cursor.execute("BEGIN;")

        # Create a schema named analytics in case of non-existence
        cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS hw6.analytics;""")

        # Create a summary table from 2 tables in raw_data schema using JOIN
        cursor.execute(f"""CREATE OR REPLACE TABLE {destination} AS(
            SELECT DISTINCT u.*, t.TS
            FROM {table_name1} u
            JOIN {table_name2} t
            ON t.sessionid = u.sessionid
        )""")
        cursor.execute("COMMIT;")
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'Build_Summary',
    start_date = datetime(2024,10,23),
    catchup=False,
    tags=['ELT'],
    schedule = '30 2 * * *'
) as dag:
    source_table1 = "hw6.raw_data.user_session_channel"
    source_table2 = "hw6.raw_data.session_timestamp"
    destination_table = "hw6.analytics.session_summary"
    cur = return_snowflake_conn()

    create_summary(source_table1, source_table2, destination_table, cur)
