"""
### Run a Databricks notebook from Airflow

Run a pre-existing notebook from Airflow.

This DAG waits for a Key to arrive in S3, based on a wildcard, and then runs some Spark code in Databricks. 
"""

import airflow
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta, datetime

# default arguments
args = {
    'owner': 'Airflow',
    'email': ['EMAIL'],
    'email_on_failure' : True,
    'depends_on_past': False,
    'databricks_conn_id': 'adb_workspace'
}

S3_CONN_ID='astro-s3-workshop'
BUCKET='astro-workshop-bucket'


# Job data
job_info=[{
    'job_id':'spark_job_one',
    'config': {
			"notebook_params": {
				'inPath': '/teams/team_one'
			}
    },
    'query': "SELECT * FROM table_one"
},
    {
    'job_id':'spark_job_two',
    'config': {
			"notebook_params": {
				'inPath': '/teams/team_two'
			}
    },
    'query': "SELECT * FROM table_two"
    },
    {
    'job_id':'spark_job_three',
    'config': {
			"notebook_params": {
				'inPath': '/teams/team_three'
			}
    },
    'query': "SELECT * FROM table_three"
    },
    {
    'job_id':'spark_job_four',
    'config': {
			"notebook_params": {
				'inPath': '/teams/team_three'
			}
    },
    'query': "SELECT * FROM table_four"
    }
    ]
with DAG(dag_id='adb_pipeline', 
default_args=args, 
start_date=datetime(2019, 1, 1), 
schedule_interval='30 4 * * *',
catchup=False) as dag:

    t1 = DummyOperator(task_id='kick_off_dag')

    t2 = S3KeySensor(
        task_id='check_for_file',
        bucket_key='globetelecom/copy_*',
        poke_interval=45,
        timeout=600,
        wildcard_match=True,
        bucket_name=BUCKET,
        aws_conn_id=S3_CONN_ID)

    for job in job_info:
        spark = DatabricksRunNowOperator(
            task_id=job['job_id'],
            job_id=job['job_id'],
            json=job['config'])

        query = PostgresOperator(
            task_id='post_{0}_query'.format(job['job_id']),
            sql=job['query'],
            postgres_conn_id='prod_postgres'
        )
        t1 >> t2 >> spark >> query
