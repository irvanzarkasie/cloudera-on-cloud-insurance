#
# Airflow: insurance medallion pipeline with parallel stages where dependencies allow.
#
# Orchestrates CDE jobs 01–07 only (DQ jobs 08/09 run separately). job_name = {owner}_01_..., etc.
# Airflow task_id is the suffix only (no username) for a cleaner graph.
#
# Set WORKSHOP_USER on the Airflow host if resolve_username(None) is not the workload user.

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

from workshop_config import resolve_username

owner = resolve_username(None)


def cde_job(task_id: str, cde_job_name: str) -> CDEJobRunOperator:
    """task_id = Airflow node; cde_job_name = exact name registered in CDE."""
    return CDEJobRunOperator(
        task_id=task_id,
        dag=dag,
        job_name=cde_job_name,
        retries=2,
    )


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

# job_name must match CDE registration: {username}_<script_suffix>
load_customers = cde_job(
    "01_load_customers_iceberg",
    f"{owner}_01_load_customers_iceberg",
)
load_claims = cde_job(
    "02_load_claims_iceberg",
    f"{owner}_02_load_claims_iceberg",
)
silver_customers = cde_job(
    "03_silver_customers",
    f"{owner}_03_silver_customers",
)
silver_claims = cde_job(
    "04_silver_claims",
    f"{owner}_04_silver_claims",
)
silver_enriched = cde_job(
    "05_silver_claims_enriched",
    f"{owner}_05_silver_claims_enriched",
)
gold_kpi = cde_job(
    "06_gold_kpi_by_state",
    f"{owner}_06_gold_kpi_by_state",
)
gold_trends = cde_job(
    "07_gold_trends_and_watchlist",
    f"{owner}_07_gold_trends_and_watchlist",
)

start >> [load_customers, load_claims]
load_customers >> silver_customers
load_claims >> silver_claims
[silver_customers, silver_claims] >> silver_enriched
silver_enriched >> [gold_kpi, gold_trends]
[gold_kpi, gold_trends] >> end
