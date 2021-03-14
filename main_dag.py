import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from main import main

args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2020, 2, 13),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(dag_id='main_dag', default_args=args, schedule_interval=None) as dag:
    parse_vk_wall = PythonOperator(
        task_id='count_words',
        python_callable=main,
        dag=dag
    )