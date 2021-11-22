"""
Example DAG demonstrating the usage of BranchDayOfWeekOperator.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator

with DAG(
    dag_id="example_weekday_branch_operator",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    schedule_interval="@daily",
) as dag:
    # [START howto_operator_day_of_week_branch]
    dummy_task_1 = DummyOperator(task_id='branch_true', dag=dag)
    dummy_task_2 = DummyOperator(task_id='branch_false', dag=dag)

    branch = BranchDayOfWeekOperator(
        task_id="make_choice",
        follow_task_ids_if_true="branch_true",
        follow_task_ids_if_false="branch_false",
        week_day="Monday",
    )

    # Run dummy_task_1 if branch executes on Monday
    branch >> [dummy_task_1, dummy_task_2]
    # [END howto_operator_day_of_week_branch]
