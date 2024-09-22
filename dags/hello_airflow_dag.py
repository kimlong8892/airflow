from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time

# Hàm Python sẽ được gọi khi DAG chạy
def hello_airflow():
    # Gửi yêu cầu GET tới API
    response_api = requests.get('https://khiphach.net/wp-json/wp/v2/posts')
    response_api_test = requests.get('https://khiphach.net/wp-json/wp/v2/posts')

    print(response_api.json())
    print(response_api_test.json())

# Định nghĩa các tham số cơ bản của DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
for i in range(10):
    with DAG(
        dag_id='hello_airflow_dag' + str(i),
        default_args=default_args,
        schedule_interval='*/1 * * * *',  # Lịch chạy mỗi 1 phút
        catchup=False,  # Không chạy lại các ngày trước đó
        tags=['example'],
    ) as dag:

        # Định nghĩa task sử dụng PythonOperator
        hello_task = PythonOperator(
            task_id='hello_task',
            python_callable=hello_airflow,
        )

        hello_task_1 = PythonOperator(
            task_id='hello_task_1',
            python_callable=hello_airflow,
        )

        # Cài đặt thứ tự task
        hello_task >> hello_task_1