"""
Example DAG demonstrating the usage of BranchPythonOperator with depends_on_past=True, where tasks may be run
or skipped on alternating runs.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


def should_run(**kwargs):
    """
    Determine which dummy_task should be run based on if the execution date minute is even or odd.

    :param dict kwargs: Context
    :return: Id of the task to run
    :rtype: str
    """
    print(
        '------------- exec dttm = {} and minute = {}'.format(
            kwargs['execution_date'], kwargs['execution_date'].minute
        )
    )
    if kwargs['execution_date'].minute % 2 == 0:
        return "dummy_task_1"
    else:
        return "dummy_task_2"


with DAG(
    dag_id='example_branch_dop_operator_v3',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={'depends_on_past': True},
    tags=['example'],
) as dag:
    cond = BranchPythonOperator(
        task_id='condition',
        python_callable=should_run,
    )

    dummy_task_1 = DummyOperator(task_id='dummy_task_1')
    dummy_task_2 = DummyOperator(task_id='dummy_task_2')
    cond >> [dummy_task_1, dummy_task_2]
