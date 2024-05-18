import pendulum
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook


def log_details(*args, **kwargs):
    logging.info(f"Execution date is {kwargs['ds']}")
    logging.info(f"My run id is {kwargs['run_id']}")
    previous_ds = kwargs.get('prev_ds')
    if previous_ds:
        logging.info(f"My previous run was on {previous_ds}")
    next_ds = kwargs.get('next_ds')
    if next_ds:
        logging.info(f"My next run will be {next_ds}")

dag = DAG(
    'log_details_dag_legacy',
    schedule_interval="@daily",
    start_date=pendulum.now()
)

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    provide_context=True,
    dag=dag
)
