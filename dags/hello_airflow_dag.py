from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# Hàm Python sẽ được gọi khi DAG chạy
def hello_airflow():
    response_api = requests.get('https://khiphach.net/wp-json/wp/v2/posts')
    print(response_api)

# Định nghĩa các tham số cơ bản của DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
with DAG(
    dag_id='hello_airflow_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Lịch chạy mỗi 1 phút
    catchup=False,  # Không chạy lại các ngày trước đó
    tags=['example'],
) as dag:

    # Định nghĩa task sử dụng PythonOperator
    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=hello_airflow,  # Hàm Python sẽ được gọi khi task chạy
    )

    # Cài đặt thứ tự task
    hello_task