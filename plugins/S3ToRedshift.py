import json
import random
import string
import logging
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
from airflow.models import Connection


class S3ToRedshiftOperator(BaseOperator):
    """
    S3 To Redshift Operator
    :param s3_conn_id:              The source s3 connection id.
    :type s3_conn_id:               string
    :param s3_bucket:               The source s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The source s3 key.
    :type s3_key:                   string
    :param redshift_conn_id:        The destination redshift connection id.
    :type redshift_conn_id:         string
    :param redshift_schema:         The destination redshift schema.
    :type redshift_schema:          string
    :param table:                   The destination redshift table.
    :type table:                    string
    :param copy_params:             The parameters to be included when issuing
                                    the copy statement in Redshift.
    :type copy_params:              string
    :param origin_schema:           The s3 key for the incoming data schema.
                                    Expects a JSON file with a single dict
                                    specifying column and datatype as a
                                    key-value pair. (e.g. "column1":"int(11)")
    :type origin_schema:            string
    :param schema_location:         The location of the origin schema. This
                                    can be set to 'S3' or 'Local'.
                                    If 'S3', it will expect a valid S3 Key. If
                                    'Local', it will expect a dictionary that
                                    is defined in the operator itself. By
                                    default the location is set to 's3'.
    :type schema_location:          string
    :param origin_datatype:         The incoming database type from which to
                                    convert the origin schema. Required when
                                    specifiying the origin_schema. Current
                                    possible values include "mysql".
    :type origin_datatype:          string
    :param load_type:               The method of loading into Redshift that
                                    should occur. Options are "append",
                                    "rebuild", and "upsert". Defaults to
                                    "append."
    :type load_type:                string
    :param primary_key:             *(optional)* The primary key for the
                                    destination table. Not enforced by redshift
                                    and only required if using a load_type of
                                    "upsert".
    :type primary_key:              string
    :param incremental_key:         *(optional)* The incremental key to compare
                                    new data against the destination table
                                    with. Only required if using a load_type of
                                    "upsert".
    :type incremental_key:          string
    """

    template_fields = ['s3_key', 'origin_schema']

    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id,
                 redshift_schema,
                 table,
                 copy_params=[],
                 origin_schema=None,
                 schema_location='s3',
                 origin_datatype=None,
                 load_type='append',
                 primary_key=None,
                 incremental_key=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.redshift_schema = redshift_schema
        self.table = table
        self.copy_params = copy_params
        self.origin_schema = origin_schema
        self.schema_location = schema_location
        self.origin_datatype = origin_datatype
        self.load_type = load_type
        self.primary_key = primary_key
        self.incremental_key = incremental_key

        if self.load_type.lower() not in ["append", "rebuild", "upsert"]:
            raise Exception('Please choose "append", "rebuild", or "upsert".')

        if self.schema_location.lower() not in ['s3', 'local']:
            raise Exception('Valid Schema Locations are "s3" or "local".')

    def execute(self, context):
        # Append a random string to the end of the staging table to ensure
        # no conflicts if multiple processes running concurrently.
        letters = string.ascii_lowercase
        random_string = ''.join(random.choice(letters) for _ in range(7))
        self.temp_suffix = '_astro_temp_{0}'.format(random_string)
        if self.origin_schema:
            schema = self.read_and_format()
        pg_hook = PostgresHook(self.redshift_conn_id)
        self.create_if_not_exists(schema, pg_hook)
        self.reconcile_schemas(schema, pg_hook)
        self.copy_data(pg_hook, schema)

    def read_and_format(self):
        if self.schema_location.lower() == 's3':
            hook = S3Hook(self.s3_conn_id)
            schema = (hook.get_key(self.origin_schema,
                                   bucket_name='{0}'.format(self.s3_bucket))
                      .get()['Body'].read().decode('utf-8'))

            schema = json.loads(schema.replace("'", '"'))
        else:
            schema = self.origin_schema

        schema_map = {
            "tinyint(1)": "bool",
            "float": "float4",
            "double": "float8",
            "int(11)": "int4",
            "longtext": "text",
            "bigint(21)": "int8"
        }
        if self.origin_datatype:
            if self.origin_datatype.lower() == 'mysql':
                for i in schema:
                    if schema[i] in schema_map:
                        schema[i] = schema_map[schema[i]]

        return schema

    def reconcile_schemas(self, schema, pg_hook):
        pg_query = \
            """
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = '{0}' AND table_name = '{1}';
            """.format(self.redshift_schema, self.table)

        pg_schema = dict(pg_hook.get_records(pg_query))
        diff = list(set(schema.keys()) - set(pg_schema.keys()))
        # Check length of column differential to see if any new columns exist
        if len(diff):
            for i in diff:
                alter_query = """ ALTER TABLE "{0}"."{1}" ADD COLUMN {2} {3}
                """.format(self.redshift_schema, self.table, i, schema[i])
                pg_hook.run(alter_query)
                logging.info('The new columns were:' + str(diff))
        else:
            logging.info('There were no new columns.')

    def copy_data(self, pg_hook, schema=None):
        @provide_session
        def get_conn(conn_id, session=None):
            conn = (
                session.query(Connection)
                .filter(Connection.conn_id == conn_id)
                .first())
            return conn

        def getS3Conn():
            s3_conn = get_conn(self.s3_conn_id)
            aws_key = s3_conn.extra_dejson.get('aws_access_key_id')
            aws_secret = s3_conn.extra_dejson.get('aws_secret_access_key')
            return ("aws_access_key_id={0};aws_secret_access_key={1}"
                    .format(aws_key, aws_secret))

        # Delete records from the destination table where the incremental_key
        # is greater than or equal to the incremental_key of the source table
        # and the primary key is the same.
        # (e.g. Source: {"id": 1, "updated_at": "2017-01-02 00:00:00"};
        #       Destination: {"id": 1, "updated_at": "2017-01-01 00:00:00"})

        delete_sql = \
            '''
            DELETE FROM "{rs_schema}"."{rs_table}"
            USING "{rs_schema}"."{rs_table}{rs_suffix}"
            WHERE "{rs_schema}"."{rs_table}"."{rs_pk}" =
            "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_pk}"
            AND "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_ik}" >=
            "{rs_schema}"."{rs_table}"."{rs_ik}";
            '''.format(rs_schema=self.redshift_schema,
                       rs_table=self.table,
                       rs_pk=self.primary_key,
                       rs_suffix=self.temp_suffix,
                       rs_ik=self.incremental_key)

        # Delete records from the source table where the incremental_key
        # is greater than or equal to the incremental_key of the destination
        # table and the primary key is the same. This is done in the edge case
        # where data is pulled BEFORE it is altered in the source table but
        # AFTER a workflow containing an updated version of the record runs.
        # In this case, not running this will cause the older record to be
        # added as a duplicate to the newer record.
        # (e.g. Source: {"id": 1, "updated_at": "2017-01-01 00:00:00"};
        #       Destination: {"id": 1, "updated_at": "2017-01-02 00:00:00"})

        delete_confirm_sql = \
            '''
            DELETE FROM "{rs_schema}"."{rs_table}{rs_suffix}"
            USING "{rs_schema}"."{rs_table}"
            WHERE "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_pk}" =
            "{rs_schema}"."{rs_table}"."{rs_pk}"
            AND "{rs_schema}"."{rs_table}"."{rs_ik}" >=
            "{rs_schema}"."{rs_table}{rs_suffix}"."{rs_ik}";
            '''.format(rs_schema=self.redshift_schema,
                       rs_table=self.table,
                       rs_pk=self.primary_key,
                       rs_suffix=self.temp_suffix,
                       rs_ik=self.incremental_key)

        append_sql = \
            '''
            ALTER TABLE "{0}"."{1}"
            APPEND FROM "{0}"."{1}{2}"
            FILLTARGET;
            '''.format(self.redshift_schema, self.table, self.temp_suffix)

        drop_temp_sql = \
            '''
            DROP TABLE IF EXISTS "{0}"."{1}{2}";
            '''.format(self.redshift_schema, self.table, self.temp_suffix)

        truncate_sql = \
            '''
            TRUNCATE TABLE "{0}"."{1}";
            '''.format(self.redshift_schema, self.table)

        params = '\n'.join(self.copy_params)
        base_sql = \
            """
            FROM 's3://{0}/{1}'
            CREDENTIALS '{2}'
            {3};
            """.format(self.s3_bucket,
                       self.s3_key,
                       getS3Conn(),
                       params)

        load_sql = '''COPY "{0}"."{1}" {2}'''.format(self.redshift_schema,
                                                     self.table,
                                                     base_sql)
        if self.load_type == 'append':
            pg_hook.run(load_sql)
        elif self.load_type == 'upsert':
            self.create_if_not_exists(schema, pg_hook, temp=True)
            load_temp_sql = \
                '''COPY "{0}"."{1}{2}" {3}'''.format(self.redshift_schema,
                                                     self.table,
                                                     self.temp_suffix,
                                                     base_sql)
            pg_hook.run(load_temp_sql)
            pg_hook.run(delete_sql)
            pg_hook.run(delete_confirm_sql)
            pg_hook.run(append_sql, autocommit=True)
            pg_hook.run(drop_temp_sql)
        elif self.load_type == 'rebuild':
            pg_hook.run(truncate_sql)
            pg_hook.run(load_sql)

    def create_if_not_exists(self, schema, pg_hook, temp=False):
        output = ''
        for k, v in schema.items():
            k = "{quote}{key}{quote}".format(quote='"', key=k)
            field = ' '.join([k, v])
            output += field
            output += ', '
        # Remove last comma and space after schema items loop ends
        output = output[:-2]
        if temp:
            copy_table = '{0}{1}'.format(self.table, self.temp_suffix)
        else:
            copy_table = self.table
        create_schema_query = \
            '''CREATE SCHEMA IF NOT EXISTS "{0}";'''.format(
                self.redshift_schema)
        create_table_query = \
            '''CREATE TABLE IF NOT EXISTS "{0}"."{1}" ({2})'''.format(
                self.redshift_schema,
                copy_table,
                output)
        pg_hook.run(create_schema_query)
        pg_hook.run(create_table_query)


class S3ToRedshiftPlugin(AirflowPlugin):
    name = "S3ToRedshiftPlugin"
    operators = [S3ToRedshiftOperator]
    # Leave in for explicitness
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
