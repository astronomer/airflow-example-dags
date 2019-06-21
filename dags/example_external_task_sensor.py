from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable

import json


jobs = json.loads(Variable.get('jobs'))


def create_dag(dag_id,
               schedule,
               default_args):

    def hello_world_py(*args):
        print('Hello World')

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        for job in jobs:
            t1 = ExternalTaskSensor(task_id='external_sensor_{0}'
                                    .format(job['source']),
                                    external_dag_id='etl_dag',
                                    external_task_id='talend_etl_{0}'
                                    .format(job['source']))
            for i in range(0, 10):
                t2 = PythonOperator(task_id='python_code_{0}'.format(i),
                                    python_callable=hello_world_py)

                t1 >> t2

    return dag


for job in jobs:

    dag_id = 'post_load_{0}'.format(job['source'])

    default_args = {'owner': 'airflow',
                    'start_date': datetime(2018, 1, 1)
                    }

    schedule = '@daily'

    globals()[dag_id] = create_dag(dag_id,
                                   schedule,
                                   default_args)
