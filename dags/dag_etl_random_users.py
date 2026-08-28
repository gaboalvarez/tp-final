from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    'dag_etl_random_users',
    description='Orquesta Extraccion > Transformacion > Carga',
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
) as dag:
    trigger_extraccion = TriggerDagRunOperator(
        task_id='trigger_extraccion',
        trigger_dag_id='dag_extraccion',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    trigger_transformacion = TriggerDagRunOperator(
        task_id='trigger_transformacion',
        trigger_dag_id='dag_transformacion',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    trigger_carga = TriggerDagRunOperator(
        task_id='trigger_carga',
        trigger_dag_id='dag_carga',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_extraccion >> trigger_transformacion >> trigger_carga
