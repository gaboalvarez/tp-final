import os
import boto3
from airflow import DAG
from datetime import datetime
from io import BytesIO
from airflow.operators.python import PythonOperator, get_current_context
import zipfile
import pandas as pd

MINIO_ENDPOINT = 'http://data-lake:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
BUCKET_NAME = 'staging'

def create_bucket_if_not_exists(bucket_name):
    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} ya existe.")
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} creado.")

def transform_and_save_to_datalake():
    # Crear el bucket staging si no existe
    create_bucket_if_not_exists(BUCKET_NAME)
    print(f"Bucket {BUCKET_NAME} creado o ya existe.")

    s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name='us-east-1')

    # Lee los archivos ZIP desde el bucket rawdata
    raw_bucket = 'rawdata'
    objects = s3_client.list_objects_v2(Bucket=raw_bucket)
    files = {obj['Key'] for obj in objects.get('Contents', [])}
    print(f"Archivos encontrados en {raw_bucket}: {files}")

    target_files = {
        "VendorInvoices12312016csv.zip":"VendorInvoices",
        "2017PurchasePricesDeccsv.zip": "purchasePrices",
        "BegInvFINAL12312016csv.zip":"BegInv",
        "EndInvFINAL12312016csv.zip":"EndInv",
        "PurchasesFINAL12312016csv.zip":"Purchases",
        #"SalesFINAL12312016csv.zip": "sales"
    }

    for file, dataset_name in target_files.items():
        if file in files:
            print(f"Procesando archivo: {file}")
            file_obj = s3_client.get_object(Bucket=raw_bucket, Key=file)
            zip_content = BytesIO(file_obj['Body'].read())

            with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                for zip_file in zip_ref.namelist():
                    print(f"Extrayendo archivo del ZIP: {zip_file}")
                    with zip_ref.open(zip_file) as extracted_file:
                        df = pd.read_csv(extracted_file)
                        print(f"DataFrame creado para {dataset_name} con {len(df)} filas.")
                        csv_buffer = BytesIO()
                        df.to_csv(csv_buffer, index=False)
                        csv_buffer.seek(0)
                        output_key = f"transformed_{dataset_name}.csv"
                        s3_client.put_object(
                            Bucket=BUCKET_NAME,
                            Key=output_key,
                            Body=csv_buffer.getvalue()
                        )
                        print(f"Archivo CSV guardado en: {BUCKET_NAME}/{output_key}")

                        # Pasa la ruta por XCom para la siguiente tarea
                        get_current_context()['ti'].xcom_push(key=f"{dataset_name}_csv_path", value=f"{BUCKET_NAME}/{output_key}")
        else:
            print(f"Archivo {file} no encontrado en el bucket {raw_bucket}.")

dag = DAG(
    'data_transformation_dag',
    description='DAG para transformar datos y cargarlos en MinIO',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

transform_and_save_task = PythonOperator(
    task_id='transform_and_save_to_datalake',
    python_callable=transform_and_save_to_datalake,
    dag=dag,
)

transform_and_save_task
