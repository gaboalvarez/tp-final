FROM apache/airflow:2.10.5-python3.12

RUN pip install boto3 pandas requests psycopg2-binary

USER airflow