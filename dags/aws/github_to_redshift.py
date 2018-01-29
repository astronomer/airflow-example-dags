from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from plugins.GithubPlugin.operators.github_to_s3_operator import GithubToS3Operator
from airflow.operators import S3ToRedshiftOperator
from airflow.operators.postgres_operator import PostgresOperator

default_args = {'owner': 'airflow',
                'start_date': datetime(2018, 1, 5),
                'email': ['l5t3o4a9m9q9v1w9@astronomerteam.slack.com'],
                'email_on_failure': True,
                'email_on_retry': False
                }

dag = DAG('github_to_redshift',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False
          )

aws_conn_id = 'astronomer-redsift-dev'
s3_conn_id = 'astronomer-s3'
s3_bucket = 'astronomer-workflows-dev'


drop_table_sql = \
    """
    DROP TABLE IF EXISTS github_data.open_issue_count;
    """

get_individual_open_issue_counts = \
    """
    CREATE TABLE github_data.open_issue_count AS
    (SELECT login, sum(count) as count, timestamp
     FROM
            ((SELECT
                m.login,
                count(i.id),
                cast('{{ execution_date + macros.timedelta(hours=-4) }}' as timestamp) as timestamp
            FROM github_data.astronomerio_issues i
            JOIN github_data.astronomerio_members m
            ON i.assignee_id = m.id
            WHERE i.state = 'open'
            GROUP BY m.login
            ORDER BY login)
        UNION
            (SELECT
                m.login,
                count(i.id),
                cast('{{ execution_date + macros.timedelta(hours=-4) }}' as timestamp) as timestamp
            FROM github_data."airflow-plugins_issues" i
            JOIN github_data."airflow-plugins_members" m
            ON i.assignee_id = m.id
            WHERE i.state = 'open'
            GROUP BY m.login
            ORDER BY login)
        )
    GROUP BY login, timestamp);
    """

# Copy command params.
copy_params = ["COMPUPDATE OFF",
               "STATUPDATE OFF",
               "JSON 'auto'",
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]

# Github Endpoints
endpoints = [{"name": "issues",
              "payload": {"state": "all"},
              "load_type": "rebuild"},
             {"name": "members",
              "payload": {},
              "load_type": "rebuild"}]

# Github Orgs (cut a few out for faster)
orgs = [{'name': 'astronomerio',
         'github_conn_id': 'astronomerio-github'},
        {'name': 'airflow-plugins',
         'github_conn_id': 'astronomerio-github'}]

with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    finished_api_calls = DummyOperator(task_id='finished_api_calls')

    drop_table_sql = PostgresOperator(task_id='drop_table_sql',
                                      sql=drop_table_sql,
                                      postgres_conn_id=aws_conn_id)

    github_transforms = PostgresOperator(task_id='github_transforms',
                                         sql=get_individual_open_issue_counts,
                                         postgres_conn_id=aws_conn_id)

    for endpoint in endpoints:
        for org in orgs:
            github = GithubToS3Operator(task_id='github_{0}_data_from_{1}_to_s3'
                                        .format(endpoint['name'], org['name']),
                                        github_conn_id=org['github_conn_id'],
                                        github_org=org['name'],
                                        github_repo='all',
                                        github_object=endpoint['name'],
                                        payload=endpoint['payload'],
                                        s3_conn_id=s3_conn_id,
                                        s3_bucket=s3_bucket,
                                        s3_key='github/{0}/{1}.json'
                                        .format(org['name'], endpoint['name']))

            redshift = S3ToRedshiftOperator(task_id='github_{0}_from_{1}_to_redshift'.format(endpoint['name'], org['name']),
                                            s3_conn_id=s3_conn_id,
                                            s3_bucket=s3_bucket,
                                            s3_key='github/{0}/{1}.json'.format(
                                                org['name'], endpoint['name']),
                                            origin_schema='github/{0}_schema.json'.format(
                                                endpoint['name']),
                                            load_type='rebuild',
                                            copy_params=copy_params,
                                            redshift_schema='github_data',
                                            table='{0}_{1}'.format(
                                                org['name'], endpoint['name']),
                                            redshift_conn_id=aws_conn_id
                                            )

            kick_off_dag >> github >> redshift >> finished_api_calls
    finished_api_calls >> drop_table_sql >> github_transforms
