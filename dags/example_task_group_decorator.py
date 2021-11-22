"""Example DAG demonstrating the usage of the @taskgroup decorator."""

from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG


# [START howto_task_group_decorator]
# Creating Tasks
@task
def task_start():
    """Dummy Task which is First Task of Dag"""
    return '[Task_start]'


@task
def task_1(value):
    """Dummy Task1"""
    return f'[ Task1 {value} ]'


@task
def task_2(value):
    """Dummy Task2"""
    return f'[ Task2 {value} ]'


@task
def task_3(value):
    """Dummy Task3"""
    print(f'[ Task3 {value} ]')


@task
def task_end():
    """Dummy Task which is Last Task of Dag"""
    print('[ Task_End  ]')


# Creating TaskGroups
@task_group
def task_group_function(value):
    """TaskGroup for grouping related Tasks"""
    return task_3(task_2(task_1(value)))


# Executing Tasks and TaskGroups
with DAG(
    dag_id="example_task_group_decorator", start_date=datetime(2021, 1, 1), catchup=False, tags=["example"]
) as dag:
    start_task = task_start()
    end_task = task_end()
    for i in range(5):
        current_task_group = task_group_function(i)
        start_task >> current_task_group >> end_task

# [END howto_task_group_decorator]
