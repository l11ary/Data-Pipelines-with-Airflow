from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 create_sql="",
                 insert_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.insert_sql = insert_sql
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Dropping the table {self.table} in redshift")
        redshift.run(f"DROP table if exists {self.table}")
        
        self.log.info(f"Creating the table {self.table} in redshift")
        redshift.run(format(self.create_sql))
        
        self.log.info("Clearing the data from staging table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        
        self.log.info("Loading data to fact table")
        sql_statement = 'INSERT INTO %s %s' % (self.table, self.insert_sql) 
        redshift.run(sql_statement)