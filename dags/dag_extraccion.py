import io
import requests
import boto3
import os
import pandas as pd
from botocore.config import Config
from botocore.exceptions import NoCredentialsError, EndpointConnectionError
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pathlib import Path

MINIO_ENDPOINT = f"http://{os.getenv('DATALAKE_CONTAINER_NAME')}:9000"
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
URL = 'https://randomuser.me/api/?results=2&gender=female&nat=es'
BUCKET_RAW = 'rawdata'
TXT_PATH = Path(__file__).resolve().parent / "last_execution.txt"

# Configuración de S3 con timeouts estrictos para evitar conexiones colgadas
boto_config = Config(
    signature_version='s3v4',
    connect_timeout=5,
    read_timeout=10,
    retries={'max_attempts': 2}
)

def verificar_conexion_minio():
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=AWS_REGION,
        config=boto_config
    )
    response = s3_client.list_buckets()
    print("Conexión a MinIO exitosa!")
    print("Buckets:", [bucket['Name'] for bucket in response['Buckets']])
    return True

def extraccion_datalake():
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=AWS_REGION,
        config=boto_config
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_filename = f"RAW_random_users_{timestamp}.csv"

    try:
        s3_client.head_bucket(Bucket=BUCKET_RAW)
        print(f"Bucket {BUCKET_RAW} ya existe")
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_RAW)
        print(f"Bucket {BUCKET_RAW} creado")
    
    # Agregamos timeout=10 a la petición web
    response = requests.get(URL, timeout=10)
    response.raise_for_status()

    data = response.json()
    parsed_users = []
    for user in data['results']:
        parsed_users.append({
            'uuid': user['login']['uuid'],
            'gender': user['gender'],
            'title': user['name']['title'],
            'first_name': user['name']['first'],
            'last_name': user['name']['last'],
            'email': user['email'],
            'username': user['login']['username'],
            'phone': user['phone'],
            'cell': user['cell'],
            'id_type': user['id']['name'],
            'id_value': user['id']['value'],
            'dob_date': user['dob']['date'],
            'age': user['dob']['age'],
            'street_number': user['location']['street']['number'],
            'street_name': user['location']['street']['name'],
            'city': user['location']['city'],
            'state': user['location']['state'],
            'country': user['location']['country'],
            'postcode': user['location']['postcode'],
            'latitude': user['location']['coordinates']['latitude'],
            'longitude': user['location']['coordinates']['longitude'],
            'nat': user['nat'],
            'picture_large': user['picture']['large']
        })

    df = pd.DataFrame(parsed_users)

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8')
    csv_buffer.seek(0)

    s3_client.upload_fileobj(csv_buffer, BUCKET_RAW, raw_filename)
    print(f"Se procesaron {len(parsed_users)} usuarios y se guardaron en {BUCKET_RAW}/{raw_filename}.")

    TXT_PATH.write_text(raw_filename, encoding="utf-8")
    print(f"Nombre guardado en {TXT_PATH}: {raw_filename}")

    return True

dag = DAG(
    'dag_extraccion',
    description='DAG para descargar archivos y subir a Datalake',
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
)

tarea_conexion_minio = PythonOperator(
    task_id='conexion_minio',
    python_callable=verificar_conexion_minio,
    dag=dag,
)

tarea_extraccion = PythonOperator(
    task_id='extraccion',
    python_callable=extraccion_datalake,
    dag=dag,
)

tarea_conexion_minio >> tarea_extraccion