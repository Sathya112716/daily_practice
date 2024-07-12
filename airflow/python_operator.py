from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_python_function():
    print("Hello from Python!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 12),
    'retries': 1,
}

with DAG('python_operator_example', default_args=default_args, schedule_interval='@daily') as dag:

    python_task = PythonOperator(
        task_id='python_task',
        python_callable=my_python_function
    )
