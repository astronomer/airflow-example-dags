from airflow.plugins_manager import AirflowPlugin
from .operators import TestOperator

class AstronomerPlugin(AirflowPlugin):
    name = "astronomer_plugin"
    operators = [
        TestOperator
    ]
