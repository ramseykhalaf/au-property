from datetime import datetime
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

@task
def print_execution_date(execution_date):
    print(f"Execution date: {execution_date}")


with DAG(
        'my_monthly_dag',
        default_args=default_args,
        description='A monthly DAG',
        start_date=datetime(2023, 1, 1),  # Start date of the DAG
        schedule_interval='0 0 1 * *',  # Run on the first day of every month
) as dag:

    print_execution_date(execution_date="{{ ds_nodash }}")
