from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Định nghĩa DAG
default_args = {
    'start_date': datetime(2023, 9, 23),
    'retries': 1
}

with DAG('php_dag', default_args=default_args, schedule_interval='@once') as dag:
    # Sử dụng BashOperator để chạy tập lệnh PHP
    run_php_script = BashOperator(
        task_id='run_php_script',
        bash_command='php /dag/php.php'
    )

    run_php_script
