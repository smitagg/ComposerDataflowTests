#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
    BeamRunPythonPipelineOperator,
)
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



START_DATE = datetime(2022, 9, 21)
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
output_path = "gs:intense-wares-362802/User_OUT.csv",


# define the python function
def my_function(x):
    return x + " is a must have tool for Data Engineers."
    
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataflow/index.html#airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator
with models.DAG(
    "wordCount_template_v1",
    default_args=default_args,
    start_date=START_DATE,
    catchup=False,
    schedule_interval='@once',  # Override to match your needs
    tags=['example'],
) as dag_template:
    pipelineOperator = BeamRunPythonPipelineOperator(
        task_id="wordcount-job",
        py_file="gs://intense-wares-362802-us/wordCount.py",     
        pipeline_options={},
        py_options=[],
        py_interpreter="python3.8",
        py_system_site_packages=True,
        runner="DataflowRunner",
        dataflow_config=DataflowConfiguration(
            job_name="wordcount-job",
            project_id="intense-wares-362802",
            location="us-east1",
        ),
        default_pipeline_options={
            "temp_location": "gs://intense-wares-362802-us/temp/",
            "staging_location": "gs://intense-wares-362802-us/staging/", 
        },
    )

pipelineOperator


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
