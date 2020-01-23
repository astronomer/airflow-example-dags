# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.sensors.external_task_sensor import ExternalTaskSensor
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.models import Variable
# from airflow.operators.slack_operator import SlackAPIPostOperator

# import json


# jobs = [{
#         "source": "source_A",
#         "destination": "destination_A",
#         "image": "image_A"
#         }, {
#         "source": "source_B",
#         "destination": "destination_B",
#         "image": "image_B"
#         }, {
#         "source": "source_C",
#         "destination": "destination_C",
#         "image": "image_A"
#         }, {
#         "source": "source_C",
#         "destination": "destination_C",
#         "image": "image_C"
#         }, {
#         "source": "source_D",
#         "destination": "destination_D",
#         "image": "image_D"
#         }, {
#         "source": "source_E",
#         "destination": "destination_E",
#         "image": "image_E"
#         }, {
#         "source": "source_F",
#         "destination": "destination_F",
#         "image": "image_F"
#         }, {
#         "source": "source_G",
#         "destination": "destination_G",
#         "image": "image_G"
#         }, {
#         "source": "source_H",
#         "destination": "destination_H",
#         "image": "image_H"
#         }, {
#         "source": "source_I",
#         "destination": "destination_I",
#         "image": "image_I"
#         }, {
#         "source": "source_J",
#         "destination": "destination_J",
#         "image": "image_J"
#         }, {
#         "source": "source_K",
#         "destination": "destination_K",
#         "image": "image_K"
#         }]


# def create_dag(dag_id,
#                schedule,
#                default_args,
#                job):

#     dag = DAG(dag_id,
#               schedule_interval=schedule,
#               default_args=default_args,
#               catchup=False,
#               max_active_runs=1)

#     with dag:
#         start = DummyOperator(task_id='start')
#         t1 = ExternalTaskSensor(task_id='wait_for_talend_{0}'
#                                 .format(job['source']),
#                                 external_dag_id='etl_dag',
#                                 external_task_id='talend_etl_{0}'
#                                 .format(job['source']))
#         t2 = SlackAPIPostOperator(task_id='post_slack_{0}'.format(job['source']),
#                                   username='ETL',
#                                   slack_conn_id='slack_conn',
#                                   text="My job {0} finished".format(
#                                       job['source']),
#                                   channel='biz-cloud-billing')

#         start >> t1 >> t2

#         if job['source'] == 'source_B':
#             for i in range(0, 15):
#                 m = DummyOperator(task_id='TeamB_alerting_logic_{0}'.format(i))

#                 t2 >> m

#     return dag


# for job in jobs:

#     dag_id = 'post_load_{0}'.format(job['source'])

#     default_args = {'owner': 'airflow',
#                     'start_date': datetime(2018, 1, 1)
#                     }

#     schedule = '@daily'

#     globals()[dag_id] = create_dag(dag_id,
#                                    schedule,
#                                    default_args,
#                                    job)
