from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

# Hàm Python sẽ được gọi khi DAG chạy
def hello_airflow():
    # Gửi yêu cầu GET tới API
    response_api = requests.get('https://khiphach.net/wp-json/wp/v2/posts')

    # Kiểm tra nếu yêu cầu thành công (status code 200)
    if response_api.status_code == 200:
        # Chuyển đổi phản hồi JSON thành đối tượng Python
        data = response_api.json()

        # In ra dữ liệu
        print(data)
    else:
        # In ra lỗi nếu không thành công
        print(f"Failed to retrieve data. Status code: {response_api.status_code}")

# Định nghĩa các tham số cơ bản của DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Khởi tạo DAG
with DAG(
    dag_id='hello_airflow_dag_2',
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