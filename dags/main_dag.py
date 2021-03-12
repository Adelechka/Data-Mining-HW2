from datetime import datetime
from os import system

from airflow import DAG

default_args = {
    'owner': 'admin',
}

with DAG(dag_id='main_dag', default_args=default_args,
         description='post parse', start_date=datetime.today(), schedule_interval=None) as dag:
    @dag.task()
    def start():
        system("python main.py")
