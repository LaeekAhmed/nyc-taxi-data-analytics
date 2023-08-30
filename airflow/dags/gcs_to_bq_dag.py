import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = 'nyc_data_lake'
BIGQUERY_DATASET = 'nyc_trips_data'
# changed

DATASET = "tripdata"
TAXI_COLORS = {'yellow': 'tpep_pickup_datetime', 'green': 'lpep_pickup_datetime'}

default_args = {
   "owner": "airflow",
   "start_date": days_ago(1),
   "depends_on_past": False,
   "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
   dag_id="gcs_to_bq_dag",
   default_args=default_args,
   catchup=False,
   max_active_runs=1,
   tags=['dtc-de'],
) as dag:

   for taxi_color, ds_col in TAXI_COLORS.items():
      
      # arrange the files in the right sequence :
      gcs_2_gcs_task = GCSToGCSOperator(
         task_id=f'move_{taxi_color}_{DATASET}_files_task',
         source_bucket=BUCKET,
         source_object=f'raw/{taxi_color}_*',
         destination_bucket=BUCKET,
         destination_object=f'{taxi_color}/',
         move_object=False # copy or move the files, we want to copy
      )

      # create an external table in BigQuery based on the parquet files :
      gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
         task_id=f"bq_{taxi_color}_{DATASET}_external_table_task",
         table_resource={
               "tableReference": {
                  "projectId": PROJECT_ID,
                  "datasetId": BIGQUERY_DATASET,
                  "tableId": f"{taxi_color}_{DATASET}_external_table",
               },
               "externalDataConfiguration": {
                  "autodetect": "True",
                  "sourceFormat": "PARQUET",
                  "sourceUris": [f"gs://{BUCKET}/{taxi_color}/*"],
               },
         },
      )

      CREATE_BQ_TBL_QUERY = (
         f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{taxi_color}_{DATASET} \
         PARTITION BY DATE({ds_col}) \
         AS \
         SELECT * FROM {BIGQUERY_DATASET}.{taxi_color}_{DATASET}_external_table;"
      )

      # create a partitioned table based on the external table :
      bq_ext_2_part_task = BigQueryInsertJobOperator(
         task_id=f"bq_create_{taxi_color}_{DATASET}_partitioned_table_task",
         configuration={
               "query": {
                  "query": CREATE_BQ_TBL_QUERY,
                  "useLegacySql": False,
               }
         }
      )

      gcs_2_gcs_task >> gcs_2_bq_ext_task >> bq_ext_2_part_task
