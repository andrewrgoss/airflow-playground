from datetime import datetime
from typing import Any, Dict

import httpx

from airflow.decorators import dag, task
from airflow.models.baseoperator import BaseOperator
from airflow.operators.email import EmailOperator


class GetRequestOperator(BaseOperator):
    """Custom operator to sand GET request to provided url"""

    def __init__(self, *, url: str, **kwargs):
        super().__init__(**kwargs)
        self.url = url

    def execute(self, context):
        return httpx.get(self.url).json()


# [START dag_decorator_usage]
@dag(schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False, tags=['example'])
def example_dag_decorator(email: str = 'example@example.com'):
    """
    DAG to send server IP to email.

    :param email: Email to send IP to. Defaults to example@example.com.
    :type email: str
    """
    get_ip = GetRequestOperator(task_id='get_ip', url="http://httpbin.org/get")

    @task(multiple_outputs=True)
    def prepare_email(raw_json: Dict[str, Any]) -> Dict[str, str]:
        external_ip = raw_json['origin']
        return {
            'subject': f'Server connected from {external_ip}',
            'body': f'Seems like today your server executing Airflow is connected from IP {external_ip}<br>',
        }

    email_info = prepare_email(get_ip.output)

    EmailOperator(
        task_id='send_email', to=email, subject=email_info['subject'], html_content=email_info['body']
    )


dag = example_dag_decorator()
# [END dag_decorator_usage]
