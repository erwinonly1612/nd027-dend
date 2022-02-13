from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_table = """
        INSERT INTO {table}
        {select_sql}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 truncate="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.truncate = truncate

    def execute(self, context):
        """
        transform staging tables into fact table - songplays
        
        parameters:
            - redshift_conn_id: redshift cluster connection
            - table: fact table name
            - truncate: clean table before loading
            - select_query: select query from SqlQueries
        """
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # table exists
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
        self.log.info(f'Load {self.table}')
        self.log.info(f'Load {self.select_sql}')
        load_sql = LoadFactOperator.insert_table.format(
            table = self.table,
            select_sql = self.select_sql  
        )
        redshift.run(load_sql)