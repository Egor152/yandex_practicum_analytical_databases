from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum


AWS_ACCESS_KEY_ID = "Скрыл по просьбе Яндекс Практикума"
AWS_SECRET_ACCESS_KEY = "Скрыл по просьбе Яндекс Практикума"

def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='Скрыл по просьбе Яндекс Практикума',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
				Bucket=bucket, 
				Key=key, 
				Filename=f'/data/{key}'
)


bash_command_tmpl = """
head {{ params.files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
#Функция загрузки csv-файлов в папку data
def sprint6_project_dag_get_data():
    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv' ) #Список csv-файлов с данными
    #Форирование списка задач в Airflow
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}', #Форирование задачи в airflow
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'Скрыл по просьбе Яндекс Практикума', 'key': key},
        ) for key in bucket_files
    ]
    #Задача, которая выводит 10 первых строк в csv-файлах
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )

    fetch_tasks >> print_10_lines_of_each

_ = sprint6_project_dag_get_data()


