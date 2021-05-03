from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 aws_conn_id = 'aws_credentials',
                 table = 'dummy_table',
                 file_type ='json',
                 s3_bucket ='',
                 s3_key = '',
                 json_path = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.file_type = file_type
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        """
        This operatior takes data from S3 storage and put it into staging tables which from it's parsed to target tables.
        :param context: specified above
        :return: staging tables
        """
        self.log.info(f'Staging phase start to table: {self.table}')
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(
            SqlQueries.truncate_table.format(self.table)
        )
        redshift_hook.run(
            SqlQueries.copy_tables_to_stage.format(
                self.table,
                self.s3_bucket,
                self.s3_key,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
        )
        self.log.info(f'Staging phase finished to table: {self.table}')




