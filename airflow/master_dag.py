import airflow.utils.dates
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

# Instantiate the DAG object
dag = DAG(
    'master_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=3),
    catchup=False
)

sensor = ExternalTaskSensor(
    task_id='sensor',
    external_dag_id='slave_dag',
    external_task_id='t1',
    dag=dag,
    execution_delta=timedelta(minutes=2)
)

last_task = EmptyOperator(
    task_id="last_task",
    dag=dag
)

sensor >> last_task
