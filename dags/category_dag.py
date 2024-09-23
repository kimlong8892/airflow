from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import requests
from airflow.models import Variable

def get_data_from_db():
    hook = MySqlHook(mysql_conn_id='laravel_db')
    connection = hook.get_conn()

    with connection.cursor() as cursor:
        cursor.execute("SELECT c.*, d.url as website_url, d.dag_id as website_name FROM crawl_categories c INNER JOIN website_dags d ON c.website_dag_id = d.id")
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

    connection.close()
    result = [dict(zip(columns, row)) for row in data]

    return result

def get_posts(row):
    url_api_laravel = Variable.get("laravel_api") + '/get-posts-from-category'
    print(url_api_laravel)
    list_category = requests.get(url_api_laravel, params = row)
    print(list_category.json())

def create_dag(dag_id, schedule_interval, row):
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2023, 9, 20),
        'retries': 1,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False,
        tags=[row['website_name']],
        is_paused_upon_creation=False
    )

    with dag:
        task_get_posts = PythonOperator(
            task_id='task_get_posts',
            python_callable=get_posts,
            op_kwargs={'row': row}
        )

        task_get_posts

    return dag

data = get_data_from_db()
for i, row in enumerate(data):
    dag_id = str(row['slug']) + "_" + str(row['id'])
    schedule_interval = '0 * * * *'
    globals()[dag_id] = create_dag(dag_id, schedule_interval, row)