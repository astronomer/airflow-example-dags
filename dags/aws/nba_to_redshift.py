from airflow import DAG
from NbaPlugin.hooks.nba_hook import NbaHook
from NbaPlugin.operators.nba_to_s3_operator import NbaToS3Operator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import S3ToRedshiftOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
                'owner': 'airflow',
                'start_date': datetime(2017, 10, 19)
}

dag = DAG('nba_proj',
          default_args=default_args,
          schedule_interval='@once'
          )

def aaron_gordon():
    g = NbaHook(endpoint='player', method='PlayerCareer',
                id='203932', stats='regular_season_totals')

    return g.call()


players = {
    'kristaps_porzingis': 204001,
    'frankie_smokes': 1628373,
    'aaron_gordon': 203932,
    'greek_freak': 203507,
    'john_wall': 202322
}

methods = {'PlayerCareer': 'regular_season_totals',
           'PlayerGameLogs': 'info'}

with dag:
    kick_off_dag = DummyOperator(task_id='basketball_analysis_sideproj')
    for player in players:
        for method in methods.keys():
            to_s3 = NbaToS3Operator(
                task_id='{0}_{1}_to_s3'.format(player, method),
                player_name=player,
                endpoint='player',
                method=method,
                id=players[player],
                stats=methods[method],
                s3_conn_id='astronomer-s3',
                s3_bucket='astronomer-workflows-dev',
                s3_key='{0}_{1}.json'.format(player, method),
                )
            s3_to_redshift = S3ToRedshiftOperator(
                task_id='{0}_{1}_to_redshift'.format(player, method),
                redshift_conn_id='astronomer-redshift-dev',
                redshift_schema='viraj_testing',
                table='{player}_{stats}'.format(player=player, stats=methods[method]),
                s3_conn_id='astronomer-s3',
                s3_bucket='astronomer-workflows-dev',
                s3_key='{0}_{1}.json'.format(player, method),
                origin_schema='{0}_{1}_schema.json'.format(player, method),
                load_type='rebuild'
                )

            kick_off_dag >> to_s3 >> s3_to_redshift
