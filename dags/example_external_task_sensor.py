from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='DagA',
    default_args=default_args,
    schedule_interval='@daily'
) as daga:
    t1 = DummyOperator(task_id='dummy_task_a')

with DAG(
    dag_id='DagB',
    default_args=default_args,
    schedule_interval='@daily'
) as dagb:
    t1 = ExternalTaskSensor(task_id='external_sensor',
                            external_dag_id='DagA',
                            external_task_id='dummy_task_a')
