import os
import boto3
import io
from airflow import DAG
from datetime import datetime
from io import BytesIO
from airflow.operators.python import PythonOperator
import zipfile
import pandas as pd
import os
from pathlib import Path

MINIO_ENDPOINT = f"http://{os.getenv('DATALAKE_CONTAINER_NAME')}:9000"
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
BUCKET_RAW = 'rawdata'
BUCKET_STAGING = 'staging'
TXT_PATH = Path(__file__).resolve().parent / "last_execution.txt"

def crear_verificar_bucket_staging(s3_client,bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} ya existe.")
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} creado.")

def transformar_datos():
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name=AWS_REGION)
    
    crear_verificar_bucket_staging(s3_client,BUCKET_STAGING)

    raw_file_name = TXT_PATH.read_text(encoding="utf-8").strip()

    raw_object = s3_client.get_object(Bucket=BUCKET_RAW, Key=raw_file_name)
    df = pd.read_csv(io.BytesIO(raw_object['Body'].read()))
    print(f"Registros RAW leídos: {len(df)}")

    # A. Eliminar nulos en campos primarios
    df = df.dropna(subset=['uuid', 'email'])

    # B. Deduplicar por UUID
    df = df.drop_duplicates(subset=['uuid'], keep='first')

    # C. Limpieza y estandarización de strings
    string_cols_title = ['first_name', 'last_name', 'city', 'state', 'country', 'street_name']
    for col in string_cols_title:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    df['email'] = df['email'].astype(str).str.strip().str.lower()

    # D. Formateo de fecha de nacimiento (ISO 8601 a YYYY-MM-DD)
    df['dob_date'] = pd.to_datetime(df['dob_date'], errors='coerce').dt.strftime('%Y-%m-%d')

    # E. Casteo explícito de tipos numéricos
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    print(f"Registros procesados y limpios: {len(df)}")

    stg_file_name = raw_file_name.replace("RAW_", "STG_")

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)

    s3_client.upload_fileobj(csv_buffer, BUCKET_STAGING, stg_file_name)
    print(f"Archivo guardado en {BUCKET_STAGING}/{stg_file_name}")

    TXT_PATH.write_text(stg_file_name, encoding="utf-8")
    print(f"Nombre guardado en {TXT_PATH}: {stg_file_name}")
    return True


dag = DAG(
    'dag_transformacion',
    description='DAG para transformar datos en MinIO',
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
)

tarea_transformacion = PythonOperator(
    task_id='transformacion',
    python_callable=transformar_datos,
    dag=dag,
)

tarea_transformacion
