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
from airflow.operators.python import PythonOperator
from google.cloud import storage
import pandas as pd
from io import StringIO
import io
import sys 
from Gatekeeper.myPluginClass import myPluginClass



def download_blob_into_memory(bucket_name, blob_name):
    """Downloads a blob into memory."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(blob_name)
    contents = blob.download_as_string().decode("utf-8") 
    return contents

def upload_blob_from_text(bucket_name, blob_name, contents):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(contents)

    
START_DATE = datetime(2022, 10, 28)
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


def check_data(inputData1,inputData2):
    adfData = download_blob_into_memory('intense-wares-362802','Input/'+inputData1)
    rdfData = download_blob_into_memory('intense-wares-362802','Input/'+inputData2)
    adfStringIO = StringIO(adfData)
    rdfStringIO = StringIO(rdfData)
    
    adf = pd.read_csv(adfStringIO, sep=",")
    rdf = pd.read_csv(rdfStringIO, sep=",")
    for i, Roles in adf['userRoles'].items():
        rList = Roles.split('|')
        l = [ r for r in rList if r in rdf['userRoles'].values]
        adf.loc[i,'userRoles']= '|'.join(l)       
    s = io.StringIO()
    adf.to_csv(s)
    upload_blob_from_text( 'intense-wares-362802','Input/USER_OUT.csv', s.getvalue() )
    upload_blob_from_text( 'intense-wares-362802','Input/USER_OUT_1.csv', adf.to_string(index=False) )

def check_data1(inputData1,inputData2):
    print(sys.path)

    adf = pd.read_csv('gs://intense-wares-362802/Input/Users.csv', sep=",")
    rdf = pd.read_csv('gs://intense-wares-362802/Input/Roles.csv', sep=",")
    for i, Roles in adf['userRoles'].items():
        rList = Roles.split('|')
        l = [ r for r in rList if r in rdf['userRoles'].values]
        adf.loc[i,'userRoles']= '|'.join(l)       
    s = io.StringIO()
    adf.to_csv(s)
    adf.to_csv ("gs://intense-wares-362802/Input/USER_OUT_3.csv", index = None, header=True)
    
    p=myPluginClass("SmitaG")
    
 #   upload_blob_from_text( 'intense-wares-362802','Input/USER_OUT.csv', s.getvalue() )
  #  upload_blob_from_text( 'intense-wares-362802','Input/USER_OUT_2.csv', adf.to_string(index=False) )

# ['/opt/python3.8/bin', '/opt/python3.8/lib/python38.zip', '/opt/python3.8/lib/python3.8', '/opt/python3.8/lib/python3.8/lib-dynload', '/opt/python3.8/lib/python3.8/site-packages', '/home/airflow/gcs/dags', '/etc/airflow/config', '/home/airflow/gcs/plugins']

with models.DAG(
    "template_2114__", #+ datetime.now().strftime("%H%M%S"),
    default_args=default_args,
    start_date=START_DATE,
    catchup=False,
    schedule_interval='0 * * * *',  # Override to match your needs
    tags=['example'],
) as dag_template:


    check_data = PythonOperator(
        task_id='check_data',
        provide_context=True,
        python_callable=check_data1,
        op_kwargs={"inputData1":'Users.csv',"inputData2":'Roles.csv'},
        dag=dag_template,
    )
    
    
    
    
    
    
    
    
 ##  [START howto_operator_start_template_job]
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
    

    # pipelineOperator = BeamRunPythonPipelineOperator(
        # task_id="ingest_template_v6",
        # py_file="gs://intense-wares-362802/ingest.py",     
        # pipeline_options={},
        # py_options=[],
        # py_interpreter="python3.8",
        # runner='DataflowRunner',
        # py_system_site_packages=True,
        # default_pipeline_options={
            # "temp_location": "gs://intense-wares-362802/Temp/",
            # "staging_location": "gs://intense-wares-362802/Staging/", 
        # },
    # )

#pipelineOperator
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
