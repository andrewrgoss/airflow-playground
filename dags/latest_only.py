"""Example of the LatestOnlyOperator"""

import datetime as dt

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.latest_only import LatestOnlyOperator

with DAG(
    dag_id='latest_only',
    schedule_interval=dt.timedelta(hours=4),
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['example2', 'example3'],
) as dag:
    latest_only = LatestOnlyOperator(task_id='latest_only')
    task1 = DummyOperator(task_id='task1')

    latest_only >> task1
