from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import requests

def get_data_from_db():
    # Kết nối đến MySQL và truy vấn dữ liệu
    hook = MySqlHook(mysql_conn_id='laravel_db')
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dags")
    data = cursor.fetchall()
    cursor.close()
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
        'start_date': datetime(2023, 9, 20),  # Đặt lại giá trị cho start_date
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