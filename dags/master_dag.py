from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    'master_dag',
    description='Orquesta ingestion -> transformation -> load',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    trigger_ingestion = TriggerDagRunOperator(
        task_id='trigger_ingestion',
        trigger_dag_id='data_ingestion_dag',
        wait_for_completion=True,
    )
    trigger_transformation = TriggerDagRunOperator(
        task_id='trigger_transformation',
        trigger_dag_id='data_transformation_dag',
        wait_for_completion=True,
    )
    trigger_load = TriggerDagRunOperator(
        task_id='trigger_load',
        trigger_dag_id='database_loader_dag',
        wait_for_completion=True,
    )

    trigger_ingestion >> trigger_transformation >> trigger_load
