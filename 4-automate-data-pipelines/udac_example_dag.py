from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    def create_tables():
        redshift_hook = PostgresHook('redshift')
        sql_queries = open('/workspace/home/airflow/dags/cd0031-automate-data-pipelines/project/starter/create_tables.sql','r').read()
        redshift_hook.run(sql_queries)

    start_operator = DummyOperator(task_id='Begin_execution')

    create_tables = PythonOperator(
        task_id = 'Create_tables',
        python_callable = create_tables
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table = "staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_path = "s3://udacity-dend/log_data",
        json_path="s3://udacity-dend/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_path = "s3://udacity-dend/song_data",
        json_path="auto"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        redshift_conn_id="redshift",
        load_sql_stmt=final_project_sql_statements.SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        truncate = True,
        redshift_conn_id="redshift",
        load_sql_stmt=final_project_sql_statements.SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        truncate = True,
        redshift_conn_id="redshift",
        load_sql_stmt=final_project_sql_statements.SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        truncate = True,
        redshift_conn_id='redshift',
        load_sql_stmt=final_project_sql_statements.SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        truncate = True,
        redshift_conn_id='redshift',
        load_sql_stmt=final_project_sql_statements.SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=[ "songplays", "songs", "artists",  "time", "users"]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
     
    run_quality_checks >> end_operator

final_project_dag = final_project()
