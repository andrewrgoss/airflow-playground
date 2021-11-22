"""Used for unit tests"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(dag_id='test_utils', schedule_interval=None, tags=['example']) as dag:
    task = BashOperator(
        task_id='sleeps_forever',
        bash_command="sleep 10000000000",
        start_date=days_ago(2),
        owner='airflow',
    )
