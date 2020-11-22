from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


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

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = DummyOperator(task_id='Stage_events', dag=dag)

stage_songs_to_redshift = DummyOperator(task_id='Stage_songs', dag=dag)

load_songplays_table = DummyOperator(task_id='Load_songplays_fact_table', dag=dag)

load_user_dimension_table = DummyOperator(task_id='Load_user_dim_table', dag=dag)

load_song_dimension_table = DummyOperator(task_id='Load_song_dim_table', dag=dag)

load_artist_dimension_table = DummyOperator(task_id='Load_artist_dim_table', dag=dag)

load_time_dimension_table = DummyOperator(task_id='Load_time_dim_table', dag=dag)

run_quality_checks = DummyOperator(task_id='Run_data_quality_checks', dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift, ] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator

