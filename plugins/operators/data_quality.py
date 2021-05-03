from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 sql_test = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_test = sql_test

    def execute(self, context):
        """
        This function count rows in destinaton tables and flag if records are populated.
        :param context: defined above
        :return: log messages
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for test in self.sql_test:
            record = redshift_hook.get_records(self.sql_test)
            if record == 0:
                self.log.info(f'zero records running: {self.sql_test}')
            else:
                self.log.info(f'records present running: {self.sql_test}')

