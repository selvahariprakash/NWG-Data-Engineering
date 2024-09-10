from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    load_sql_template = """
        INSERT INTO {table}
        {load_sql_stmt};
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                table = "",
                load_sql_stmt = "",
                truncate = False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt
        self.truncate = truncate

    def execute(self, context):
        self.log.info('LoadDimensionOperator starting')

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        if self.truncate:
            self.log.info('Truncate')
            redshift.run(f"truncate {self.table}")

        load_sql = LoadDimensionOperator.load_sql_template.format(
            table = self.table,
            load_sql_stmt = self.load_sql_stmt
        )
        redshift.run(load_sql)

        self.log.info('LoadDimensionOperator completed')