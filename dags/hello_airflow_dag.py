from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import time
import pymysql

def get_data_from_db():
    # Replace with actual logic to connect and query the database
    connection = pymysql.connect(
        host='0.0.0.0',
        user='admin',
        password='Admin123',
        database='laravel_db',
        cursorclass=DictCursor
    )
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dags")
    data = cursor.fetchall()
    connection.close()
    return data

def hello_airflow():
    response_api = requests.get('https://khiphach.net/wp-json/wp/v2/posts')
    response_api_test = requests.get('https://khiphach.net/wp-json/wp/v2/posts')

    print(response_api.json())
    print(response_api_test.json())

def create_dag(dag_id, schedule_interval):
    default_args = {
        'owner': 'airflow',
        'start_date': days_ago(1),
        'retries': 1,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False,
        tags=['dynamic'],
    )

    with dag:
        hello_task = PythonOperator(
            task_id='hello_task',
            python_callable=hello_airflow,
        )
        hello_task

    return dag


data = get_data_from_db()
for i, row in enumerate(data):
    dag_id = f"{row['dag_id']}"
    schedule_interval = row['schedule_interval']
    globals()[dag_id] = create_dag(dag_id, schedule_interval)

#
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 20),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# # Khởi tạo DAG
# for i in range(10):
#     with DAG(
#         dag_id='hello_airflow_dag' + str(i),
#         default_args=default_args,
#         schedule_interval='*/1 * * * *',  # Lịch chạy mỗi 1 phút
#         catchup=False,  # Không chạy lại các ngày trước đó
#         tags=['example'],
#     ) as dag:
#
#         # Định nghĩa task sử dụng PythonOperator
#         hello_task = PythonOperator(
#             task_id='hello_task',
#             python_callable=hello_airflow,
#         )
#
#         hello_task_1 = PythonOperator(
#             task_id='hello_task_1',
#             python_callable=hello_airflow,
#         )
#
#         # Cài đặt thứ tự task
#         hello_task >> hello_task_1