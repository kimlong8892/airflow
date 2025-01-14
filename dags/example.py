from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

def create_file(**kwargs):
    print("File has been created at /tmp/example_file.txt")

def write_to_file(**kwargs):
    print("Content has been written to the file.")

def delete_file(**kwargs):
    print("File does not exist.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG',
    schedule_interval='@daily',  # Chạy mỗi ngày
    catchup=False,
) as dag:
    create_file_task = PythonOperator(
        task_id='create_file',
        python_callable=create_file,
    )

    write_file_task = PythonOperator(
        task_id='write_to_file',
        python_callable=write_to_file,
    )

    delete_file_task = PythonOperator(
        task_id='delete_file',
        python_callable=delete_file,
    )

    create_file_task >> write_file_task >> delete_file_task
