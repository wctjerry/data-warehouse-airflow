from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 sql_test="",
                 expected_result="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql_test=sql_test
        self.expected_result=expected_result
        self.redshift_conn_id=redshift_conn_id

    def execute(self, context):
        self.log.info('Checking data quality')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        result = redshift.get_first(self.sql_test)
        result = result[0]
        
        if result != self.expected_result:
            self.log.info(f'Generated result: {result}')
            raise ValueError("Data validation fails")