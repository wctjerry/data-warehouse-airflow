from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    stage_sql_template = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        JSON '{json_setting}'
        REGION 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_setting="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_setting = json_setting
        

    def execute(self, context):
        self.log.info("Copying data from S3 to Redshift")
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        formatted_sql = self.stage_sql_template.format(
            table=self.table,
            s3_path=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            json_setting=self.json_setting
        )
        redshift.run(formatted_sql)





