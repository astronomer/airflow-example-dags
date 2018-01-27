from airflow.plugins_manager import AirflowPlugin
from NbaPlugin.hooks.nba_hook import NbaHook
from NbaPlugin.operators.nba_to_s3_operator import NbaToS3Operator


class NbaPlugin(AirflowPlugin):
    name = "nba-plugin"
    operators = [NbaToS3Operator]
    hooks = [NbaHook]
