"""DAG de prueba: falla al instante y envía email. Configura conexión smtp_default en Airflow."""

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def fail_immediately():
    raise RuntimeError("Test: DAG falló a propósito para probar alerta por email")


with DAG(
    "test_email_alert_dag",
    description="Falla rápido para probar email_on_failure",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "email_on_failure": False,
    },
) as dag:
    PythonOperator(
        task_id="fail_task",
        python_callable=fail_immediately,
    )
