from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl_dag',
          schedule_interval='@once',
          default_args=default_args)

jobs = json.loads(Variable.get('jobs'))

with dag:

    start = DummyOperator(task_id="start")

    for job in jobs:
        k = KubernetesPodOperator(
            task_id="talend_etl_{0}".format(job['source']),
            namespace='',
            image="talend_job_{0}".format(job['image']),
            labels={"foo": "bar"},
            name="talend_etl_{0}".format(job['source']),
            in_cluster=True,
            get_logs=True)

        start >> k
