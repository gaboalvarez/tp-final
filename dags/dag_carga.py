import os
import io
import boto3
from airflow import DAG
from datetime import datetime
from io import BytesIO
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine,text
from pathlib import Path

MINIO_ENDPOINT = f"http://{os.getenv('DATALAKE_CONTAINER_NAME')}:9000"
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
BUCKET_STAGING = 'staging'
BUCKET_MASTER = 'masterdata'
TXT_PATH = Path(__file__).resolve().parent / "last_execution.txt"

POSTGRES_HOST = os.getenv('APP_POSTGRES_CONTAINER') or 'data-db'
POSTGRES_DB = os.getenv('APP_POSTGRES_DB')
POSTGRES_USER = os.getenv('APP_POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('APP_POSTGRES_PASSWORD')
BUCKET_NAME = 'staging'


s3_client = boto3.client('s3',
                             endpoint_url=MINIO_ENDPOINT,
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY,
                             region_name=AWS_REGION)


def cargar_datos():
    if not TXT_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo {TXT_PATH}.")

    stg_file_name = TXT_PATH.read_text(encoding="utf-8").strip()
    timestamp_folder = stg_file_name.replace("STG_random_users_", "").replace(".csv", "")
    prefix = f"random_users/{timestamp_folder}/"

    print(f"Buscando particiones en {BUCKET_MASTER}/{prefix}")

    response = s3_client.list_objects_v2(Bucket=BUCKET_MASTER, Prefix=prefix)

    if 'Contents' not in response:
        print(f"No se encontraron particiones en la ruta '{prefix}'.")
        return

    particiones = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]

    print(f"\n--- Particiones encontradas ({len(particiones)}) ---")
    for part in particiones:
        print(f" -> {part}")

    db_url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}'
    engine = create_engine(db_url)

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM usuarios WHERE execution_timestamp = :ts"),
            {"ts": timestamp_folder}
        )
        print(f"Registros con execution_timestamp='{timestamp_folder}' eliminados si existían.")

    for part_key in particiones:
        print(f"Cargando partición: {part_key}...")
        
        file_obj = s3_client.get_object(Bucket=BUCKET_MASTER, Key=part_key)
        df_part = pd.read_parquet(io.BytesIO(file_obj['Body'].read()), engine='pyarrow')
        df_part['execution_timestamp'] = timestamp_folder

        df_part.to_sql(
            name='usuarios',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500
        )
        print(f"  -> Partición {part_key} cargada correctamente ({len(df_part)} filas).")

    print(f"\nProceso finalizado. Todas las particiones del lote '{timestamp_folder}' fueron procesadas.")

def particionar_datos():
    if not TXT_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo {TXT_PATH}.")
    
    stg_file_name = TXT_PATH.read_text(encoding="utf-8").strip()
    print(f"Archivo STG a leer: {stg_file_name}")

    stg_object = s3_client.get_object(Bucket=BUCKET_STAGING, Key=stg_file_name)
    df = pd.read_csv(io.BytesIO(stg_object['Body'].read()))
    total_records = len(df)
    print(f"Registros totales leídos de Staging: {total_records}")

    # Filas 0-49 -> Partición 0 | Filas 50-99 -> Partición 1 | etc.
    df['chunk_partition'] = df.index // 50

    try:
        s3_client.head_bucket(Bucket=BUCKET_MASTER)
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_MASTER)

    base_file_name = stg_file_name.replace("STG_", "MASTER_").replace(".csv", "")
    timestamp_folder = base_file_name.replace("MASTER_random_users_", "")

    for part_num, group_df in df.groupby('chunk_partition'):
        parquet_buffer = io.BytesIO()

        # Removemos la columna auxiliar de particionado
        clean_group_df = group_df.drop(columns=['chunk_partition'])

        clean_group_df.to_parquet(
            parquet_buffer, 
            index=False, 
            engine='pyarrow', 
            compression='snappy'
        )
        parquet_buffer.seek(0)

        # Ruta formateada: random_users/TIMESTAMP/MASTER_random_users_TIMESTAMP_part_X.parquet
        master_key = f"random_users/{timestamp_folder}/{base_file_name}_part_{part_num}.parquet"

        s3_client.upload_fileobj(parquet_buffer, BUCKET_MASTER, master_key)
        print(f"Partición {part_num} guardada en: {BUCKET_MASTER}/{master_key}")


dag = DAG(
    'dag_carga',
    description='DAG para cargar datos desde MinIO a PostgreSQL',
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
)

tarea_particion = PythonOperator(
    task_id='particion',
    python_callable=particionar_datos,
    dag=dag,
)

tarea_carga_datos = PythonOperator(
    task_id='carga',
    python_callable=cargar_datos,
    dag=dag,
)

tarea_particion >> tarea_carga_datos 