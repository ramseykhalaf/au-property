from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 5, 1),
}

with DAG(
        'example_dag',
        default_args=default_args,
        description='An example DAG using the @task decorator',
        schedule_interval=timedelta(days=1),
        catchup=False,
) as dag:

    @task(task_id="print_hello_world")
    def _print_hello_world():
        print("Hello, World!")

    print_hello_world = _print_hello_world()

    print_hello_world
