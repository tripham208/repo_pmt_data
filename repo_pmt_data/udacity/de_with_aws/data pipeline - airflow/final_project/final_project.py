from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from stage_redshift import StageToRedshiftOperator
from load_fact import LoadFactOperator
from load_dimension import LoadDimensionOperator
from data_quality import DataQualityOperator
import final_project_sql_statements

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    end_operator = EmptyOperator(task_id='End_execution')

    start_operator >> stage_songs_to_redshift
    start_operator >> stage_events_to_redshift

    stage_songs_to_redshift >> load_songplays_table
    stage_events_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_songplays_table >> load_artist_dimension_table

    load_time_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
