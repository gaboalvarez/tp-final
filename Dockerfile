FROM apache/airflow:3.0.0-python3.12

RUN pip install boto3 pandas requests psycopg2-binary

USER airflow