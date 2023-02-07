# -*- encoding: utf-8 -*-
"""
Filename: final_project.py
Author: Mingxing Jin
Contact: Mingxing.Jin@bmw-brilliance.cn
Description: Loading data from s3 to redshift, transform data to fact and dimension and check the data quality.
"""


from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from airflow.operators.postgres_operator import PostgresOperator
from udacity.common import final_project_sql_statements


#
# The following DAG performs the following functions:
#       1. Create related tables
#       2. Loading data from S3 to RedShift
#       3. Transform data to facts and dimention
#       4. Performs a data quality check on the time table in RedShift
#       5. Configure the task dependencies
#  


# Set up default args based on requirements of projects
default_args = {
    'owner': 'Mingxing Jin',
    'depends_on_past' : False,
    'start_date': pendulum.now(),
    'retries' : 3,
    'retry_delay' : timedelta(minutes=5),
    'catchup' : False,
    'email_on_failure' : False
}


# Set up default args,descrition and schedule interval base on requirements of projects
@dag(
    default_args=default_args,
    description='Load data from S3 to Redshift, transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)


def final_project():

    # Begin execution
    start_operator = DummyOperator(task_id='Begin_execution')

    # Create related tables, once run once
#     create_related_table = PostgresOperator(
#     task_id="create_related_table",
#     postgres_conn_id="redshift",
#     sql="create_tables.sql"
# )

    # Load data from S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="buckets-data-pipelines-mingxingjin",
        s3_key="log-data",
        s3_path="s3://buckets-data-pipelines-mingxingjin/log-data",
        json_option="auto"


    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="buckets-data-pipelines-mingxingjin",
        s3_key="song-data",
        s3_path="s3://buckets-data-pipelines-mingxingjin/song-data/A/A/A",
        json_option="auto"
    )


    # Transform data to fact and dimention
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql=final_project_sql_statements.SqlQueries.songplay_table_insert,
        truncate=False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        sql=final_project_sql_statements.SqlQueries.user_table_insert,
        truncate=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        sql=final_project_sql_statements.SqlQueries.song_table_insert,
        truncate=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        sql=final_project_sql_statements.SqlQueries.artist_table_insert,
        truncate=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql=final_project_sql_statements.SqlQueries.time_table_insert,
        truncate=False
    )

    # Data quality check
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        table="songs"
    )

    # End execution
    end_operator = DummyOperator(task_id='End_execution')

    # Configure the task dependencies based on requirements of projects
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