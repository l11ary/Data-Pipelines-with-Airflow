from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    #copy command for copying songs from s3 to redshift
    copy_songsql_json  = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            REGION 'us-west-2'
            FORMAT AS JSON 'auto'
        """
    #copy command for copying events from s3 to redshift
    copy_eventsql_json = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            REGION 'us-west-2'
            FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
        """
    
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 sql="",
                 log_jsonpath="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql = sql

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Creating the table {self.table} in redshift")
        redshift.run(format(self.sql))
        
        self.log.info("Clearing the data from staging table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        

        if self.table == "staging_songs":
            formatted_sql = StageToRedshiftOperator.copy_songsql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
            
        else:
            formatted_sql = StageToRedshiftOperator.copy_eventsql_json.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key
            )
        
        redshift.run(formatted_sql)





