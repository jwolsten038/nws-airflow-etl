from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from etl.nws_huntsville_etl import run

with DAG(
    dag_id="nws_huntsville_hourly_etl",
    start_date=datetime(2026, 1, 23),
    schedule="@hourly",
    catchup=False,
    tags=["etl", "weather"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_nws_huntsville_etl",
        python_callable=run,
    )
