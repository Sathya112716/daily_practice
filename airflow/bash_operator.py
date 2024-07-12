from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 12),
    'retries': 1,
}

with DAG('bash_operator_example', default_args=default_args, schedule_interval='@daily') as dag:

    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Hello, World!"'
    )
