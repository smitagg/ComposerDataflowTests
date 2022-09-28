"""
Example Airflow DAG for Google Cloud Dataflow service
"""
import os
from datetime import datetime
from typing import Callable, Dict, List
from urllib.parse import urlparse

from airflow import models
from airflow.exceptions import AirflowException
from airflow.providers.apache.beam.operators.beam import (
    BeamRunJavaPipelineOperator,
    BeamRunPythonPipelineOperator
)
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.operators.dataflow import (
    CheckJobRunning,
    DataflowTemplatedJobStartOperator,DataflowConfiguration
)
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

#airflow.providers.google.cloud.operators.dataflow.DataflowConfiguration
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


START_DATE = datetime(2022, 9, 22)
GCS_TMP = os.environ.get('GCP_DATAFLOW_GCS_TMP', 'gs://intense-wares-362802/temp/')
GCS_STAGING = os.environ.get('GCP_DATAFLOW_GCS_STAGING', 'gs://intense-wares-362802/staging/')
GCS_OUTPUT = os.environ.get('GCP_DATAFLOW_GCS_OUTPUT', 'gs://intense-wares-362802/output')

default_args = {
    'dataflow_default_options': {
        'tempLocation': GCS_TMP,
        'stagingLocation': GCS_STAGING,
        'project' : 'My First Project',
        'project_id' : 'intense-wares-362802',
        'zone': 'us-east1-b',
        'location':'us-east1'

    }
}

output_table = 'Dataset_2.newTable_User_OUT',
output_path = "gs://intense-wares-362802/User_OUT.csv",

input_path = "gs://intense-wares-362802/User_OUT.csv",

DATASET_NAME = 'Dataset_1'
# with models.DAG(
     # "ingest_template_v4",
     # default_args=default_args,
     # start_date=START_DATE,
     # catchup=False,
     # schedule_interval='@once',  # Override to match your needs
     # tags=['example'],
 # ) as dag_template:
   # # start_python_job_async = DataflowCreatePythonJobOperator(
        # task_id="start-python-job-async",
        # py_file="gs://europe-west3-testenvironmen-c7ddef17-bucket/dags/wordCount.py",
        # py_options=[],
        # job_name='{{task.task_id}}',
        # options={
            # 'output': "gs://europe-west3-testenvironmen-c7ddef17-bucket/out.csv",
        # },
        # py_requirements=['apache-beam[gcp]==2.25.0'],
        # py_interpreter='python3',
        # py_system_site_packages=False,
        # location='europe-west3',
        # wait_until_finished=False,
    # )
# start_python_job_async

##1 .read from GCS bucket 2 .csv files 


# ##working
# with models.DAG(
     # "ingest_template_v3",
     # default_args=default_args,
     # start_date=START_DATE,
     # catchup=False,
     # schedule_interval='@once',  # Override to match your needs
     # tags=['example'],
 # ) as dag_template:
    # create_external_table = BigQueryCreateExternalTableOperator(
    # task_id="create_external_table",
    # destination_project_dataset_table=f"{DATASET_NAME}.external_table_cra",
    # bucket="intense-wares-362802",
    # source_objects=["User.csv"],
    # autodetect=True
    # # schema_fields=[
        # # {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
        # # {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
    # # ],
# )
# create_external_table


##2. query and join to make a single file
##3. write data to CSV file in GCS

    # bq_recent_questions_query = bigquery_operator.BigQueryOperator(
        # task_id='bq_recent_questions_query',
        # sql="""
        # SELECT owner_display_name, title, view_count
        # FROM `bigquery-public-data.stackoverflow.posts_questions`
        # WHERE creation_date < CAST('{max_date}' AS TIMESTAMP)
            # AND creation_date >= CAST('{min_date}' AS TIMESTAMP)
        # ORDER BY view_count DESC
        # LIMIT 100
        # """.format(max_date=max_query_date, min_date=min_query_date),
        # use_legacy_sql=False,
        # destination_dataset_table=bq_recent_questions_table_id)
    # # [END composer_bigquery]

    # # Export query result to Cloud Storage.
    # export_questions_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        # task_id='export_recent_questions_to_gcs',
        # source_project_dataset_table=bq_recent_questions_table_id,
        # destination_cloud_storage_uris=[output_file],
        # export_format='CSV')

  
# # https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataflow/index.html#airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator
with models.DAG(
    "ingest_template_v6",
    default_args=default_args,
    start_date=START_DATE,
    catchup=False,
    schedule_interval='0 * * * *',  # Override to match your needs
    tags=['example'],
) as dag_template:
    pipelineOperator = BeamRunPythonPipelineOperator(
        task_id="ingest_template_v6",
        py_file="gs://intense-wares-362802/ingest.py",     
        pipeline_options={},
        py_options=[],
        py_interpreter="python3.8",
        runner='DataflowRunner',
        py_system_site_packages=True,
        default_pipeline_options={
            "temp_location": "gs://intense-wares-362802/Temp/",
            "staging_location": "gs://intense-wares-362802/Staging/", 
        },
    )

#pipelineOperator


    # [START howto_operator_start_template_job]
    # start_template_job = DataflowTemplatedJobStartOperator(
        # task_id="start-template-job",
        # template='gs://dataflow-templates/latest/GCS_Text_to_BigQuery',
        
        # parameters={
                    # 'javascriptTextTransformGcsPath' : "gs://intense-wares-362802/Input/Jsfunction.js",
                    # 'JSONPath' : "gs://intense-wares-362802/Input/JsonFile.json",
                    # 'javascriptTextTransformFunctionName' : "transform",
                    # 'outputTable' : "intense-wares-362802:Dataset_2.newTable_User",
                    # 'inputFilePattern' : "gs://intense-wares-362802/User.csv",
                    # 'bigQueryLoadingTemporaryDirectory' : "gs://intense-wares-362802/Temp",  
                     # 'write_disposition' : "WRITE_TRUNCATE",                    
        # },
    # )
    


    # [END howto_operator_start_template_job]
    # write_to_final = BigQueryOperator(
        # task_id=f"write_to_final",
        # sql="select * from Dataset_2.newTable_User",
        # use_legacy_sql=False,
        # priority="BATCH",
        # write_disposition="WRITE_TRUNCATE",
        # destination_dataset_table=output_table,
        # params={
            # "event_dataset": EVENT_DATASET,
            # "days_to_query": 30,
        # },
        # bigquery_conn_id="google_cloud_default",
        # labels={"team": "test"},
        # dag=dag
    # )
    # export_to_gcs = BigQueryToCloudStorageOperator(
        # task_id=f"export_to_gcs",
        # source_project_dataset_table=output_table,
        # destination_cloud_storage_uris=output_path,
        # compression="NONE",
        # export_format="CSV",
        # bigquery_conn_id="google_cloud_default",
        # labels={"team": "test"},
        # dag=dag,
    # )
