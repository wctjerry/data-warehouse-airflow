from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator)

from helpers import SqlQueries

default_args = {
    'owner': 'wctjerry',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once', # For test use
#           schedule_interval='@daily', # For regular schedule
        )

sql_queries = SqlQueries()

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# stage_events_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_events',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_events",
#     s3_bucket="udacity-dend",
#     s3_key="log_data/{{execution_date.year}}/{{execution_date.month}}/{{ds}}-events.json",
#     json_setting="s3://udacity-dend/log_json_path.json",
# )

# stage_songs_to_redshift = StageToRedshiftOperator(
#     task_id='Stage_songs',
#     dag=dag,
#     redshift_conn_id="redshift",
#     aws_credentials_id="aws_credentials",
#     table="staging_songs",
#     s3_bucket="udacity-dend",
#     s3_key="song_data",
#     json_setting="auto",
# )

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql_insert_source=sql_queries.songplay_table_insert,
    sql_insert_columns="playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent",
    redshift_conn_id="redshift",
    table="songplays",
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql_insert_source=sql_queries.user_table_insert,
    sql_insert_columns="userid, first_name, last_name, gender, level",
    redshift_conn_id="redshift",
    table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql_insert_source=sql_queries.song_table_insert,
    sql_insert_columns="songid, title, artistid, year, duration",
    redshift_conn_id="redshift",
    table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql_insert_source=sql_queries.artist_table_insert,
    sql_insert_columns="artistid, name, location, lattitude, longitude",
    redshift_conn_id="redshift",
    table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    sql_insert_source=sql_queries.time_table_insert,
    sql_insert_columns="start_time, hour, day, week, month, year, weekday",
    redshift_conn_id="redshift",
    table="time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    sql_test="SELECT SUM(CASE WHEN userid IS NULL THEN 1 ELSE 0 END) FROM users",
    expected_result=0,
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# start_operator >> [stage_songs_to_redshift, stage_events_to_redshift, ] >> load_songplays_table # For production use
start_operator >> load_songplays_table # For test use, no need to load staging during every test, which is expensive
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

