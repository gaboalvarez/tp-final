import io
import requests
import boto3
from botocore.exceptions import NoCredentialsError, EndpointConnectionError
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

urls = [
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/PurchasesFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/BegInvFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/2017PurchasePricesDeccsv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/VendorInvoices12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/EndInvFINAL12312016csv.zip",
    "https://www.pwc.com/us/en/careers/university_relations/data_analytics_cases_studies/SalesFINAL12312016csv.zip"
]

def check_minio_connection():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://data-lake:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            region_name='us-east-1',
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

def ingest_data_to_datalake():
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://data-lake:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            region_name='us-east-1',
            config=boto3.session.Config(signature_version='s3v4')
        )

        bucket_name = 'rawdata'
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} ya existe")
        except:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} creado")

        for url in urls:
            file_name = url.split("/")[-1]
            print(f"Procesando: {file_name}")
            
            # Descargar archivo
            response = requests.get(url)
            if response.status_code == 200:
                file_content = io.BytesIO(response.content)
                s3_client.upload_fileobj(file_content, bucket_name, file_name)
                print(f"Archivo {file_name} subido exitosamente")
            else:
                print(f"Error al descargar {file_name}. Código: {response.status_code}")

    except Exception as e:
        print(f"Error en el proceso: {e}")
        raise

dag = DAG(
    'data_ingestion_dag',
    description='DAG para descargar archivos y subir a Datalake',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

check_connection_task = PythonOperator(
    task_id='check_connection',
    python_callable=check_minio_connection,
    dag=dag,
)

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data_to_datalake,
    dag=dag,
)

check_connection_task >> ingest_task