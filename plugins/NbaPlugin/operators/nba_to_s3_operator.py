
from airflow.models import BaseOperator
from NbaPlugin.hooks.nba_hook import NbaHook
from airflow.hooks import S3Hook
import boa
import json
from os import path


class NbaToS3Operator(BaseOperator):
    """
    NBA To S3 Operator

    """
    pass

    def __init__(self,
                 endpoint,
                 id,
                 player_name,
                 method,
                 stats,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 payload=None,
                 *args,
                 **kwargs):
            super().__init__(*args, **kwargs)
            self.endpoint = endpoint
            self.player_name = player_name
            self.id = id
            self.method = method
            self.stats = stats
            self.s3_conn_id = s3_conn_id
            self.s3_bucket = s3_bucket
            self.s3_key = s3_key
            self.payload = payload

    def get_data(self):
        return NbaHook(endpoint=self.endpoint, method=self.method,
                       id=self.id, stats=self.stats).call()

    def schemaMapping(self, fields):
        schema = {}
        for field in fields:
            if type(fields[field]) == int:
                schema[boa.constrict(field)] = 'INTEGER'
            elif type(fields[field]) == str:
                schema[boa.constrict(field)] = 'VARCHAR'
            elif type(fields[field]) == float:
                schema[boa.constrict(field)] = 'FLOAT'
        print(schema)
        return schema

    def execute(self, context):

        response = self.get_data()
        response.columns = response.columns.map(boa.constrict)

        json_data = json.loads(response.to_json(orient='records'))
        schema_map = self.schemaMapping(json_data[0])

        s3 = S3Hook(s3_conn_id=self.s3_conn_id)

        if self.s3_key.endswith('.json'):
            split = path.splitext(self.s3_key)
            schema_key = '{0}_schema{1}'.format(split[0], split[1])

        results = [dict([boa.constrict(k), v]
                        for k, v in i.items()) for i in json_data]
        results = '\n'.join([json.dumps(i) for i in results])

        s3.load_string(
            string_data=str(schema_map),
            bucket_name=self.s3_bucket,
            key=schema_key,
            replace=True
        )

        s3.load_string(
            string_data=results,
            bucket_name=self.s3_bucket,
            key=self.s3_key,
            replace=True
        )
        s3.load_string
        s3.connection.close()
