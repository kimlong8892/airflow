from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import requests
from airflow.models import Variable


def get_data_from_db():
    # Kết nối đến MySQL và truy vấn dữ liệu
    hook = MySqlHook(mysql_conn_id='laravel_db')
    connection = hook.get_conn()

    with connection.cursor() as cursor:  # Sử dụng cursor bình thường
        cursor.execute("SELECT * FROM website_dags")
        data = cursor.fetchall()  # Trả về danh sách các tuple

        # Lấy tên cột sau khi thực hiện truy vấn
        columns = [col[0] for col in cursor.description]  # Lấy tên cột

    connection.close()

    # Chuyển đổi danh sách tuple thành danh sách dictionary
    result = [dict(zip(columns, row)) for row in data]  # Tạo dictionary cho mỗi hàng
    return result


def get_categories(row):
    url_api_laravel = Variable.get("laravel_api") + '/get-categories-from-website'
    print(url_api_laravel)
    list_category = requests.get(url_api_laravel, params = {'url': row['url'], 'id': row['id']})
    print(list_category.json())


def create_dag(dag_id, schedule_interval, row):
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2023, 9, 20),  # Đặt lại giá trị cho start_date
        'retries': 1,
    }
    tags = None

    if row['tags']:
        tags = row['tags'].split(",")

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False,
        tags=tags,
        is_paused_upon_creation=False
    )

    with dag:
        task_get_categories = PythonOperator(
            task_id='task_get_categories',
            python_callable=get_categories,
            op_kwargs={'row': row}
        )

        task_get_categories

    return dag

data = get_data_from_db()
for i, row in enumerate(data):
    dag_id = f"{row['dag_id']}"
    schedule_interval = row['schedule_interval']
    globals()[dag_id] = create_dag(dag_id, schedule_interval, row)