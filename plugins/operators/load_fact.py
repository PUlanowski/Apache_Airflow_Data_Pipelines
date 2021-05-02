from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 sql = '',
                 target_table = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.target_table = target_table

    def execute(self, context):
        self.log.info(f'Loading phase start to fact table: {self.target_table}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(self.sql)
        self.log.info(f'Loading phase finished to fact table: {self.target_table}')