from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import os
import tempfile
from big_data.scripts.transform_script import transform  # Импорт функции transform

# Пути к данным
BASE_DIR = "/home/airflow/data"
PROFIT_TABLE_PATH = os.path.join(BASE_DIR, 'profit_table.csv')
FLAGS_ACTIVITY_PATH = os.path.join(BASE_DIR, 'flags_activity.csv')

# Основные параметры DAG
default_args = {
    'owner': 'Anton Gordon',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'optimized_customer_activity_etl',
    default_args=default_args,
    description='Оптимизированный ETL DAG для флагов активности клиентов',
    schedule_interval='0 0 5 * *',  # Запуск 5-го числа каждого месяца
    start_date=days_ago(1),
    catchup=False,
)

# ЗАДАЧА 1: Извлечение данных
def extract(**kwargs):
    """Извлечение данных из profit_table.csv."""
    if not os.path.exists(PROFIT_TABLE_PATH):
        raise FileNotFoundError(f"Файл {PROFIT_TABLE_PATH} не найден")
    
    # Чтение данных
    df = pd.read_csv(PROFIT_TABLE_PATH)
    print(f"Данные успешно извлечены: {len(df)} строк")
    
    # Передача данных через XCom
    kwargs['ti'].xcom_push(key="extracted_data", value=df.to_dict())

# ЗАДАЧА 2: Трансформация данных
def transform_data(**kwargs):
    """Трансформация данных."""
    # Получение данных из XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data', key='extracted_data')
    if data is None:
        raise ValueError("Данные не извлечены. Проверьте выполнение задачи 'extract_data'.")

    df = pd.DataFrame.from_dict(data)
    print(f"Получено данных для трансформации: {len(df)} строк")
    
    # Преобразование данных
    date = kwargs['ds']  # Дата выполнения DAG
    transformed_df = transform(df, date)
    print(f"Данные успешно преобразованы: {len(transformed_df)} строк")
    
    # Передача преобразованных данных через XCom
    ti.xcom_push(key="transformed_data", value=transformed_df.to_dict())

# ЗАДАЧА 3: Загрузка данных
def load_data(**kwargs):
    """Загрузка данных в flags_activity.csv."""
    # Получение преобразованных данных из XCom
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    if transformed_data is None:
        raise ValueError("Преобразованные данные отсутствуют. Проверьте выполнение задачи 'transform_data'.")

    transformed_df = pd.DataFrame.from_dict(transformed_data)
    print(f"Данные для загрузки: {len(transformed_df)} строк")

    # Проверка и объединение с существующими данными
    if os.path.exists(FLAGS_ACTIVITY_PATH):
        existing_data = pd.read_csv(FLAGS_ACTIVITY_PATH)
        combined_data = pd.concat([existing_data, transformed_df]).drop_duplicates()
    else:
        combined_data = transformed_df

    # Сохранение данных
    combined_data.to_csv(FLAGS_ACTIVITY_PATH, index=False)
    print(f"Данные успешно сохранены в {FLAGS_ACTIVITY_PATH}")

# Определение задач
task_extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Установка зависимостей
task_extract >> task_transform >> task_load
