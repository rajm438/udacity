from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    staging_copy = ("""
    copy {} 
    from {}
    access_key_id  ‘{{}}'
    secret_access_key '{{}}'
    json {}
    region 'us-west-2';
    """)
    

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table ="",
                 s3_bucket="",
                 s3_key="",
                 copy_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_format = copy_format
        
 
    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        #redshift_hook = ProstgresHook(postgres_conn_id=self."redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        tgt_table =self.target_table
        cpy_format = self.copy_format
        rendered_key = self.s3_key
        s3_path="s3://{}//{}".format(self.s3_bucket,rendered_key)
        #execution_date = kwargs["execution_date"]
        sql_stmt = StageToRedshiftOperator.staging_copy.format(tgt_table,s3_path,credentials.access_key,credentials.secret_key,cpy_format)
        
        redshift_hook.run(sql_stmt)




