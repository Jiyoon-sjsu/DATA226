from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():
  hook = SnowflakeHook(snowflake_conn_id='snowflake_conn', warehouse='compute_wh', database='dev', schema='raw_data')
  conn = hook.get_conn()
  return conn.cursor()

@task
def extract(url):
  f = requests.get(url).json()
  return (f)

@task
def return_last_90d_price(extract_data, stock_symbol):
  results = []
  for d in extract_data["Time Series (Daily)"]:
    stock_info = extract_data["Time Series (Daily)"][d]
    stock_info["symbol"] = stock_symbol
    stock_info["date"] = d
    results.append(stock_info)
  return results

@task
def load_records(cur, records, table):
  try:
    cur.execute("BEGIN;")
    cur.execute(f"CREATE OR REPLACE TABLE {table} (open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume INT, symbol VARCHAR, date DATE, PRIMARY KEY (date, symbol));")
    for r in records:
      open = r["1. open"]
      high = r["2. high"]
      low = r["3. low"]
      close = r["4. close"]
      volume = r["5. volume"]
      symbol = r["symbol"]
      date = r["date"]
      insert_sql = f"INSERT INTO {table} (open, high, low, close, volume, symbol, date) VALUES ({open}, {high}, {low}, {close}, {volume}, TO_CHAR('{symbol}'), TO_DATE('{date}','YYYY-MM-DD'))"
      cur.execute(insert_sql)
    cur.execute("COMMIT;")
  except Exception as e:
      cur.execute("ROLLBACK;")
      print(e)
      raise e

with DAG(
  dag_id = 'aaplstock2snowflake',
  start_date = datetime(2024,9,27),
  catchup=False,
  tags=['ETL'],
  schedule = '0 0 * * *'
) as dag:
  stock_symbol = 'AAPL'
  target_table = "dev.raw_data.stock_price"
  vantage_api_key = Variable.get('vantage_api_key')
  url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={vantage_api_key}'
  cur = return_snowflake_conn()

  data = extract(url)
  lines = return_last_90d_price(data, stock_symbol)
  load_records(cur, lines, target_table)

