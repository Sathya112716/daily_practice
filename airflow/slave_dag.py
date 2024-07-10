import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

# Instantiate the DAG object
with DAG(
    dag_id='slave_dag',
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id="t1",
        bash_command="echo 'Hello, World!'"
    )

    t1
