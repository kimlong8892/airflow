from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def create_file():
    path = "/tmp/example_file.txt"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8"):
        pass
    print(f"File has been created at {path}")

def write_to_file():
    path = "/tmp/example_file.txt"
    with open(path, "a", encoding="utf-8") as f:
        f.write("Example content\n")
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
