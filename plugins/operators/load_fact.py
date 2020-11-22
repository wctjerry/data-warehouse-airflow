from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    fact_sql_template = """
                INSERT INTO {table} ({sql_insert_columns})
                {sql_insert_source}    
    """

    @apply_defaults
    def __init__(self,
                 sql_insert_source="",
                 sql_insert_columns="",
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_insert_source = sql_insert_source
        self.sql_insert_columns = sql_insert_columns
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        self.log.info('Appending new records to the fact table')
        formatted_sql = self.fact_sql_template.format(table=self.table,
                                                      sql_insert_source=self.sql_insert_source,
                                                      sql_insert_columns=self.sql_insert_columns)
        
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        redshift.run(formatted_sql)
