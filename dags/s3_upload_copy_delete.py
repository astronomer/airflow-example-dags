"""
### Copy and Delete Data in S3

Upload files to one S3 bucket, copy it to another, and delete it.

This DAG shows how to use a Python function that calls the S3 hook to generate and copy files into S3, and then delete them. 
Additionally, this DAG is written to work with the Kubernetes Executor. As such, custom resource requests are set.


This DAG was part of an Intro to Airflow Workshop. 

"""

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os


S3_CONN_ID='astro-s3-workshop'
BUCKET='astro-workshop-bucket'

name='workshop'


def upload_to_s3(file_name):

    # Instanstiaute
    s3_hook=S3Hook(aws_conn_id=S3_CONN_ID) 
    
    # Create file
    sample_file = "{0}_file_{1}.txt".format(name, file_name) #swap your name here
    example_file = open(sample_file, "w+")
    example_file.write("Putting some data in for task {0}".format(file_name))
    example_file.close()
    
    s3_hook.load_file(sample_file, 'globetelecom/{0}'.format(sample_file), bucket_name=BUCKET, replace=True)

    


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Resources
test_config2 = {"KubernetesExecutor": 
{"request_memory": "400Mi", 
"limit_memory": "400Mi", 
"request_cpu": "400m", 
"limit_cpu": "400m"}}


# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('s3_upload_copy_delete',
         start_date=datetime(2019, 1, 1),
         max_active_runs=1,
         schedule_interval='0 12 8-14,22-28 * 6',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    t0 = DummyOperator(task_id='start')

    for i in range(0,10): # generates 10 tasks
        generate_files=PythonOperator(
            task_id='generate_file_{0}_{1}'.format(name, i), # note the task id is dynamic
            python_callable=upload_to_s3,
            op_kwargs= {'file_name': i}
        )

        copy_files = S3CopyObjectOperator(
            task_id='copy_{0}_file_{1}'.format(name,i), 
            source_bucket_key='globetelecom/{0}_file_{1}_testfile_exist.txt'.format(name, i),
            dest_bucket_key='globetelecom/copy_{0}_file_{1}.txt'.format(name, i),
            source_bucket_name=BUCKET,
            dest_bucket_name=BUCKET,
            aws_conn_id=S3_CONN_ID
        )

        delete_files = S3DeleteObjectsOperator(
            task_id='delete_{0}_file_{1}'.format(name,i),
            keys='globetelecom_copy/{0}_file_{1}'.format(name,i),
            bucket=BUCKET,
            aws_conn_id=S3_CONN_ID
        )

        t0 >> generate_files >> copy_files >> delete_files # Make sure this is indented inside the scope of the loop