from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

from airflow.operators.dummy_operator import DummyOperator
import json
import os

NAMESPACE = os.getenv('HOSTNAME').split('-schedu')[0]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(dag_id='etl_dag',
          schedule_interval='@daily',
          catchup=False,
          default_args=default_args)

# jobs = Variable.get("jobs", deserialize_json=True)
# set deseairlize to true here.

jobs = [{
        "source": "source_A",
        "destination": "destination_A",
        "image": "image_A"
        }, {
        "source": "source_B",
        "destination": "destination_B",
        "image": "image_B"
        }, {
        "source": "source_C",
        "destination": "destination_C",
        "image": "image_A"
        }, {
        "source": "source_C",
        "destination": "destination_C",
        "image": "image_C"
        }, {
        "source": "source_D",
        "destination": "destination_D",
        "image": "image_D"
        }, {
        "source": "source_E",
        "destination": "destination_E",
        "image": "image_E"
        }, {
        "source": "source_F",
        "destination": "destination_F",
        "image": "image_F"
        }, {
        "source": "source_G",
        "destination": "destination_G",
        "image": "image_G"
        }, {
        "source": "source_H",
        "destination": "destination_H",
        "image": "image_H"
        }, {
        "source": "source_I",
        "destination": "destination_I",
        "image": "image_I"
        }, {
        "source": "source_J",
        "destination": "destination_J",
        "image": "image_J"
        }, {
        "source": "source_K",
        "destination": "destination_K",
        "image": "image_K"
        }]
with dag:

    start = DummyOperator(task_id="start")

    for job in jobs:
        k = KubernetesPodOperator(
            task_id="talend_etl_{0}".format(job['source']),
            namespace='datarouter-glowing-lens-5847',
            name='datarouter-glowing-lens-5847',
            image="alpine:latest",
            in_cluster=True,
            service_account='glowing-lens-5847-worker-serviceaccount',
            get_logs=True)

        t2 = SlackAPIPostOperator(task_id='post_slack_{0}'.format(job['source']),
                                  username='ETL',
                                  slack_conn_id='slack_conn',
                                  text="My job {0} finished".format(
                                      job['source']),
                                  channel='biz-cloud-billing')

        if job['source'] == 'source_B':
            for i in range(0, 6):
                m = DummyOperator(task_id='TeamB_alerting_logic_{0}'.format(i))

                t2 >> m

        if job['source'] == 'source_K':
            for i in range(0, 3):
                m = DummyOperator(task_id='Spark_Jobs_{0}'.format(i))

                t2 >> m

        start >> k >> t2
