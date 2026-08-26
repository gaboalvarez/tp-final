import os
import boto3
from airflow import DAG
from datetime import datetime
from io import BytesIO
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine

MINIO_ENDPOINT = 'http://data-lake:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
POSTGRES_HOST = 'postgresql-data'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'amn_datawarehouse'
POSTGRES_USER = 'myuser'
POSTGRES_PASSWORD = 'mypassword'
BUCKET_NAME = 'staging'

def load_to_database():
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')

    # Lista de archivos a procesar
    target_files = {
        "transformed_VendorInvoices.csv": "VendorInvoices",
        "transformed_purchasePrices.csv": "purchasePrices",
        "transformed_BegInv.csv": "BegInv",
        "transformed_EndInv.csv": "EndInv",
        "transformed_Purchases.csv": "Purchases"
        #"transformed_sales.csv": "sales"
    }

    for file, dataset_name in target_files.items():
        try:
            print(f"Procesando archivo: {file}")
            file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file)
            csv_content = BytesIO(file_obj['Body'].read())
            chunk_size = 10000
            for chunk in pd.read_csv(csv_content, chunksize=chunk_size):
                chunk.columns = [col.lower().replace(' ', '_') for col in chunk.columns] # Falsa lectura on de fly
                
                chunk.to_sql(
                    f'raw_{dataset_name.lower()}',
                    engine,
                    if_exists='append',
                    index=False
                )
                print(f"Chunk de {len(chunk)} filas guardado en tabla raw_{dataset_name.lower()}")
            
            print(f"Procesamiento completado para {dataset_name}")
            
        except Exception as e:
            print(f"Error procesando {file}: {str(e)}")
            continue

dag = DAG(
    'database_loader_dag',
    description='DAG para cargar datos desde MinIO a PostgreSQL',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

load_to_postgres_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

load_to_postgres_task 