"""
Example DAG demonstrating ``TimeDeltaSensorAsync``, a drop in replacement for ``TimeDeltaSensor`` that
defers and doesn't occupy a worker slot while it waits
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.time_delta import TimeDeltaSensorAsync

with DAG(
    dag_id="example_time_delta_sensor_async",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    wait = TimeDeltaSensorAsync(task_id="wait", delta=timedelta(seconds=10))
    finish = DummyOperator(task_id="finish")
    wait >> finish
