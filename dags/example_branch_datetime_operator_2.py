"""
Example DAG demonstrating the usage of DateTimeBranchOperator with datetime as well as time objects as
targets.
"""
import datetime

from airflow import DAG
from airflow.operators.datetime import BranchDateTimeOperator
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="example_branch_datetime_operator",
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    schedule_interval="@daily",
)

# [START howto_branch_datetime_operator]
dummy_task_1 = DummyOperator(task_id='date_in_range', dag=dag)
dummy_task_2 = DummyOperator(task_id='date_outside_range', dag=dag)

cond1 = BranchDateTimeOperator(
    task_id='datetime_branch',
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=datetime.datetime(2020, 10, 10, 15, 0, 0),
    target_lower=datetime.datetime(2020, 10, 10, 14, 0, 0),
    dag=dag,
)

# Run dummy_task_1 if cond1 executes between 2020-10-10 14:00:00 and 2020-10-10 15:00:00
cond1 >> [dummy_task_1, dummy_task_2]
# [END howto_branch_datetime_operator]


dag = DAG(
    dag_id="example_branch_datetime_operator_2",
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    schedule_interval="@daily",
)
# [START howto_branch_datetime_operator_next_day]
dummy_task_1 = DummyOperator(task_id='date_in_range', dag=dag)
dummy_task_2 = DummyOperator(task_id='date_outside_range', dag=dag)

cond2 = BranchDateTimeOperator(
    task_id='datetime_branch',
    follow_task_ids_if_true=['date_in_range'],
    follow_task_ids_if_false=['date_outside_range'],
    target_upper=datetime.time(0, 0, 0),
    target_lower=datetime.time(15, 0, 0),
    dag=dag,
)

# Since target_lower happens after target_upper, target_upper will be moved to the following day
# Run dummy_task_1 if cond2 executes between 15:00:00, and 00:00:00 of the following day
cond2 >> [dummy_task_1, dummy_task_2]
# [END howto_branch_datetime_operator_next_day]
