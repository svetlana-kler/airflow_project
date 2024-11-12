import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# path = os.path.expanduser('~/airflow_hw') заменили на:
path = '/opt/airflow'
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 11, 6),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:
    # BashOperator, выполняющий указанную bash-команду
    test_task = BashOperator(
        task_id='test_task',
        bash_command='echo "Here we go!"',
        dag=dag,
    )

    # Генерация лучшей модели
    pipeline = PythonOperator(
        task_id = 'pipeline',
        python_callable = pipeline,
        dag = dag
    )
    # Формирование предсказания на новых данных
    predict = PythonOperator(
        task_id='predict',
        python_callable = predict,
        dag = dag
    )
    # Порядок выполнения тасок
    test_task >> pipeline >> predict
