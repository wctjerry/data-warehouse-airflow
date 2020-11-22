from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    dim_sql_insert_template = """
            INSERT INTO {table} ({sql_insert_columns})
            {sql_insert_source}
    """
    dim_sql_delete_template = """
            DELETE FROM {table}
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 sql_insert_columns="",
                 sql_insert_source="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_insert_columns = sql_insert_columns
        self.sql_insert_source = sql_insert_source
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from Dimension table')

        formatted_delete_sql = self.dim_sql_delete_template.format(table=self.table)
        redshift.run(formatted_delete_sql)

        self.log.info('Drop and insert records into the dimension table')
        
        formatted_insert_sql = self.dim_sql_insert_template.format(table=self.table,
                                                     sql_insert_columns=self.sql_insert_columns,
                                                     sql_insert_source=self.sql_insert_source)
        redshift.run(formatted_insert_sql)
