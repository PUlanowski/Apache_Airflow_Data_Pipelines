from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 sql = '',
                 target_table = '',
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.target_table = target_table
        self.truncate = truncate

    def execute(self, context):
        self.log.info(f'Loading phase start to dimension table: {self.target_table}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.truncate == True:
            redshift_hook.run(
                SqlQueries.truncate_table.format(self.table)
            )
        redshift_hook.run(self.sql)
        self.log.info(f'Loading phase finished to dimension table: {self.target_table}')