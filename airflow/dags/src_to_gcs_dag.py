import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pyar_csv
import pyarrow.parquet as pyar_pq

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = 'nyc_data_lake'
BIGQUERY_DATASET = 'nyc_trips_data'

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# `execution_date` is an airflow config variable, `.strftime(\'%Y-%m\')` yeilds '2023-02' for example:
FILE_NAME = '_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# ↪ example url : https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet


# NOTE: Required ONLY IF downloaded files are .csv :  
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    '''
    the `pyar_pq.write_table` method from the `PyArrow` lib reads data stored in `table` (which was read from the CSV file)
    & writes it into a new Parquet file with the specified name and format.
    '''
    table = pyar_csv.read_csv(src_file)
    pyar_pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE func name is not std, exec takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="src_to_gcs_dag",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2022, 1, 1),
    end_date=datetime(2022, 12, 31), 
    default_args=default_args,
    # catchup=True,
    tags=['dtc-de'],
    # max_active_runs=3,
) as dag:

    for taxi_color in ["yellow", "green"]:

        # download the files from the NYC Taxi website :
        download_dataset_task = BashOperator(
            task_id=f"download_{taxi_color}_dataset_task",
            bash_command=f"curl -sSL {URL_PREFIX}{taxi_color}{FILE_NAME} > {AIRFLOW_HOME}/{taxi_color}{FILE_NAME}"
        )

        # upload the downloaded files to GCS :
        local_to_gcs_task = PythonOperator(
            task_id=f"local_{taxi_color}_to_gcs_task",
            python_callable=upload_to_gcs,
            # arguments for the Python_callable function :
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{taxi_color}{FILE_NAME}",
                "local_file": f"{AIRFLOW_HOME}/{taxi_color}{FILE_NAME}",
            },
        )

        # remove the downloaded files after uploading to GCS :
        remove_files_task = BashOperator(
            task_id=f"remove_{taxi_color}_files_task",
            bash_command=f"rm {AIRFLOW_HOME}/{taxi_color}{FILE_NAME}"
        )

        download_dataset_task >> local_to_gcs_task >> remove_files_task