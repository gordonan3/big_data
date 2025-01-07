from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
from transform_script import transform  # Импорт функции transform

# Параметры
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

DATA_DIR = '/data'  


# Функции для ETL-процесса
def extract(**kwargs):
    """Извлечение данных из profit_table.csv."""
    file_path = os.path.join(DATA_DIR, 'profit_table.csv')
    df = pd.read_csv(file_path)
    return df.to_dict()


def transform_data(**kwargs):
    """Трансформация данных с использованием функции transform."""
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame.from_dict(data)
    transformed_df = transform(df)
    return transformed_df.to_dict()


def load(**kwargs):
    """Загрузка данных в flags_activity.csv."""
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform')
    transformed_df = pd.DataFrame.from_dict(transformed_data)
    
    output_file = os.path.join(DATA_DIR, 'flags_activity.csv')
    
    if os.path.exists(output_file):
        existing_data = pd.read_csv(output_file)
        combined_data = pd.concat([existing_data, transformed_df]).drop_duplicates()
    else:
        combined_data = transformed_df
    
    combined_data.to_csv(output_file, index=False)


# Создание DAG
with DAG(
    'etl_customer_activity',
    default_args=default_args,
    description='ETL-процесс для витрины активности клиентов',
    schedule_interval='0 0 5 * *',  # Запуск 5-го числа каждого месяца
    start_date=days_ago(1),  # Укажите реальную дату начала
    catchup=False,
) as dag:

    # Задачи
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # Установка зависимостей
    extract_task >> transform_task >> load_task
