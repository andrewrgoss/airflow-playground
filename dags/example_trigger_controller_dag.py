"""
Example usage of the TriggerDagRunOperator. This example holds 2 DAGs:
1. 1st DAG (example_trigger_controller_dag) holds a TriggerDagRunOperator, which will trigger the 2nd DAG
2. 2nd DAG (example_trigger_target_dag) which will be triggered by the TriggerDagRunOperator in the 1st DAG
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="example_trigger_controller_dag",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule_interval="@once",
    tags=['example'],
) as dag:
    trigger = TriggerDagRunOperator(
        task_id="test_trigger_dagrun",
        # Ensure this equals the dag_id of the DAG to trigger
        trigger_dag_id="example_trigger_target_dag",
        conf={"message": "Hello World"},
    )
