from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            TIMEFORMAT as 'epochmillisecs'
            {} '{}'
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_format="JSON",
                 json_path = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.file_format = file_format
        self.json_path = json_path
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        """
            Copy data from S3 buckets to redshift cluster into staging tables.
            
            parameters:
                - redshift_conn_id: redshift cluster connection
                - aws_credentials_id: AWS connection
                - table: staging table name
                - s3_bucket: S3 bucket name 
                - s3_key: S3 key files of source data
                - file_format: source file format
                - example of data path:
                    * Song data: 's3://udacity-dend/song_data/A/B/C/TRABCEI128F424C983.json'  
                    * Log data: 's3://udacity-dend/log_data/2018/11/2018-11-12-events.json'  
        """

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # table exists
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        if self.execution_date:
            # Get the year and month for the path
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            s3_path = '/'.join([s3_path, str(year), str(month)])
            
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            self.json_path
        )
        redshift.run(formatted_sql)

        