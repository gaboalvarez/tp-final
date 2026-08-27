import io
import requests
import boto3
import os
import pandas as pd
from botocore.exceptions import NoCredentialsError, EndpointConnectionError
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

MINIO_ENDPOINT = f"http://{os.getenv('DATALAKE_CONTAINER_NAME')}:9000"
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
URL = 'https://randomuser.me/api/?results=2&gender=female&nat=es'
BUCKET_RAW = 'rawdata'

def verificar_conexion_minio():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=AWS_REGION,
            config=boto3.session.Config(signature_version='s3v4')
        )

        response = s3_client.list_buckets()
        print("Conexión a MinIO exitosa!")
        print("Buckets:", [bucket['Name'] for bucket in response['Buckets']])
        return True
    except NoCredentialsError:
        print("Error: No se proporcionaron credenciales válidas!")
        return False
    except EndpointConnectionError:
        print("Error: No se puede conectar al endpoint de MinIO!")
        return False
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return False

def extraccion_datalake():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=AWS_REGION,
            config=boto3.session.Config(signature_version='s3v4')
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"RAW_random_users_{timestamp}.csv"
        Variable.set("last_raw_filename", file_name)

        try:
            s3_client.head_bucket(Bucket=BUCKET_RAW)
            print(f"Bucket {BUCKET_RAW} ya existe")
        except:
            s3_client.create_bucket(Bucket=BUCKET_RAW)
            print(f"Bucket {BUCKET_RAW} creado")
        
        response = requests.get(URL)
        if response.status_code == 200:
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

            s3_client.upload_fileobj(csv_buffer, BUCKET_RAW, file_name)
            print(f"Se procesaron {len(parsed_users)} usuarios y se guardaron en {BUCKET_RAW}/{file_name}.")

    except Exception as e:
        print(f"Error en el proceso: {e}")
        raise

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