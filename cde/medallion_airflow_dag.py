#
# Airflow: bronze loads (optional if already run) + silver/gold medallion chain.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

owner = "holuser01"

jobs = [
    f"{owner}_01_load_customers_iceberg",
    f"{owner}_02_load_claims_iceberg",
    f"{owner}_03_silver_customers",
    f"{owner}_04_silver_claims",
    f"{owner}_05_silver_claims_enriched",
    f"{owner}_08_data_quality_great_expectations",
    f"{owner}_06_gold_kpi_by_state",
    f"{owner}_07_gold_trends_and_watchlist",
]

default_args = {
    "owner": owner,
    "retry_delay": timedelta(seconds=30),
    "retries": 1,
}

dag = DAG(
    f"{owner}_insurance_medallion_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule_interval=None,
    catchup=False,
    is_paused_upon_creation=False,
)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

prev = start
for job_name in jobs:
    task = CDEJobRunOperator(
        task_id=job_name,
        dag=dag,
        job_name=job_name,
        retries=2,
    )
    prev >> task
    prev = task
prev >> end
