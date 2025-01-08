# Базовый образ Airflow
FROM apache/airflow:2.7.2

# Установка дополнительных библиотек
USER airflow
RUN pip install --no-cache-dir tqdm
RUN pip install --no-cache-dir pandas

# Копирование данных и файлов в контейнер
USER root
COPY ./data /home/airflow/data/
COPY ./scripts/transform_script.py /home/airflow/dags/
COPY ./dag.py /home/airflow/dags/

# Настройка прав доступа
RUN chown -R airflow: /home/airflow/data /home/airflow/dags

# Возвращение к пользователю airflow
USER airflow
