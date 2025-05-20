"""
## Print the number of people currently in space

This DAG pulls the number of people currently in space. The number is pulled
from XCom and was pushed by the `get_astronauts` task in the `example_astronauts` DAG.
"""

from airflow.sdk import Asset, chain, dag, task
from airflow.providers.standard.operators.bash import BashOperator


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    schedule=[Asset("current_astronauts")],
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["My First DAG!"],
)
def my_astronauts_dag():
    # Define tasks
    @task
    def print_num_people_in_space(**context) -> None:
        """
        This task pulls the number of people currently in space from XCom. The number is
        pushed by the `get_astronauts` task in the `example_astronauts` DAG.
        """

        num_people_in_space = context["ti"].xcom_pull(
            dag_id="example_astronauts",
            task_ids="get_astronauts",
            key="number_of_people_in_space",
            include_prior_dates=True,
        )

        print(f"There are currently {num_people_in_space} people in space.")

    print_reaction = BashOperator(
        task_id="print_reaction",
        bash_command="echo This is awesome!",
    )

    chain(print_num_people_in_space(), print_reaction)


# Instantiate the DAG
my_astronauts_dag()
