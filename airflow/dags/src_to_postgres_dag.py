import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv('PG_HOST') 
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
# ↪ gets the values from the ENV vars in the OS of the container made from the docker-compose file which in turn gets them from the local .env !

local_workflow = DAG(
   "LocalIngestionDag",
   schedule_interval="0 6 2 * *",
   start_date=datetime(2022, 10, 1),
   end_date=datetime(2022, 12, 31),  # End date for the scheduling
   # ↪ runs 2nd of every month from start_date at 6:00 (UTC):
   # catchup=False
   tags=['dtc-de']
)


URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# `execution_date` is an airflow config variable, `.strftime(\'%Y-%m\')` yeilds '2023-02' for example:
URL_TEMPLATE = URL_PREFIX + 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# ↪ example url : https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet

# example output file : /opt/airflow/output_2023-02.parquet (confirm using terminal in docker desktop!)
FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:
   curl_task = BashOperator(
      task_id='curl',
      bash_command=f'curl -sSL {URL_TEMPLATE} > {FILE_TEMPLATE}'
   )

   ingest_task = PythonOperator(
      task_id="ingest",
      python_callable=ingest_callable,
      op_kwargs=dict(
         user=PG_USER,
         password=PG_PASSWORD,
         host=PG_HOST,
         port=PG_PORT,
         db=PG_DATABASE,
         table_name=TABLE_NAME_TEMPLATE,
         input_file=FILE_TEMPLATE
      ),
   )

   curl_task >> ingest_task