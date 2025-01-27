import logging
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import json
import vertica_python as vp
import pandas as pd
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

conn_info = {'host': 'Скрыл по просьбе Яндекс Практикума', 
             'port': 'Скрыл по просьбе Яндекс Практикума',
             'user': 'Скрыл по просьбе Яндекс Практикума',       
             'password': 'Скрыл по просьбе Яндекс Практикума',
             'database': 'Скрыл по просьбе Яндекс Практикума',
             
            'autocommit': False
}

#Запрос для загрузки данных в таблицу users

sql_for_users = r'''TRUNCATE TABLE STV2024050748__STAGING.users;
                    COPY STV2024050748__STAGING.users(id, chat_name, registration_dt, country, age)
                    FROM LOCAL '/data/users.csv'
                    DELIMITER ',';'''

#Запрос для загрузки данных в таблицу groups

sql_for_groups = r'''TRUNCATE TABLE STV2024050748__STAGING.groups;
                     COPY STV2024050748__STAGING.groups(id, admin_id, group_name, registration_dt, is_private)
                     FROM LOCAL '/data/groups.csv'
                     DELIMITER ',';'''

#Запрос для загрузки данных в таблицу dialogs

sql_for_dialogs = r'''TRUNCATE TABLE STV2024050748__STAGING.dialogs;
                      COPY STV2024050748__STAGING.dialogs(message_id, message_ts, message_from, message_to, message, message_group)
                      FROM LOCAL '/data/dialogs.csv'
                      DELIMITER ',';'''

#Запрос для загрузки данных в таблицу group_log

sql_for_group_log = r'''TRUNCATE TABLE STV2024050748__STAGING.group_log;
                      COPY STV2024050748__STAGING.group_log(group_id, user_id, user_id_from, event, event_ts)
                      FROM LOCAL '/data/group_log.csv'
                      DELIMITER ',';'''

#Функция загрузки данных в таблицы

def stg_filling(query):
    with vp.connect(**conn_info) as conn:
        curs = conn.cursor()
        curs.execute(query)
    
        conn.commit() 

#DAG для загрузки данных в stg-слой

dag = DAG(dag_id='load_data_from_csv_fiels_to_staging',
    schedule_interval=None, 
    start_date=pendulum.parse('2022-07-13'),
    tags=['this dag for load data from csv fiels to staging'])

#Задача-заглушка
start_task = DummyOperator(task_id='start_task', dag=dag)

#Задача для загрузки данных в таблицу users
users_filling_task = PythonOperator(task_id='load_users',
                      python_callable=stg_filling,
                      op_kwargs={'query':sql_for_users},
                      dag=dag)   

#Задача для загрузки данных в таблицу groups
groups_filling_task = PythonOperator(task_id='load_groups',
                      python_callable=stg_filling,
                      op_kwargs={'query':sql_for_groups},
                      dag=dag)   

#Задача для загрузки данных в таблицу dialogs
dialogs_filling_task = PythonOperator(task_id='load_dialogs',
                      python_callable=stg_filling,
                      op_kwargs={'query':sql_for_dialogs},
                      dag=dag)   

#Задача для загрузки данных в таблицу group_log
group_log_filling_task = PythonOperator(task_id='load_group_log',
                      python_callable=stg_filling,
                      op_kwargs={'query':sql_for_group_log},
                      dag=dag)   

#Задача-заглушка
end_task = DummyOperator(task_id='end_task', dag=dag)

start_task >> [users_filling_task, groups_filling_task, dialogs_filling_task, group_log_filling_task] >> end_task