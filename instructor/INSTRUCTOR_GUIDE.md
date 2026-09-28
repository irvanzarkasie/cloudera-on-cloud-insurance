# Instructor Guide — End-to-End Insurance Workshop

## Timing (suggested)

| Step | Topic | Duration |
|------|-------|----------|
| 1–2 | Bronze load (customers + claims) | 30–45 min (claims job may run longer) |
| 3 | Medallion + GX DQ (6 jobs) | 45–60 min |
| 4 | CDW views + optional viz | 20–30 min |
| 5 | CAI forecast notebook | 30 min |
| 6 | Agent Studio demo | 20–30 min |

**Fast path:** Pre-run Steps 1–3 before class; participants start at CDW + CAI + Agent.

---

## S3 data location

`s3a://cloudera-hol-buk-99feb843/data/user/holuser01/`

- `customers.csv` — uploaded
- `claims.parquet` — upload before Step 2

---

## CDE jobs to create (holuser01)

| Job name | Script |
|----------|--------|
| `holuser01_01_load_customers_iceberg` | `01_load_customers_iceberg.py` |
| `holuser01_09_data_quality_customers` | `09_data_quality_customers.py` + Python Environment `holuser01-python-gx` |
| `holuser01_02_load_claims_iceberg` | `02_load_claims_iceberg.py` |
| `holuser01_03_silver_customers` | `03_silver_customers.py` |
| `holuser01_04_silver_claims` | `04_silver_claims.py` |
| `holuser01_05_silver_claims_enriched` | `05_silver_claims_enriched.py` |
| `holuser01_08_data_quality_great_expectations` | `08_data_quality_great_expectations.py` + same Python Environment |
| `holuser01_06_gold_kpi_by_state` | `06_gold_kpi_by_state.py` |
| `holuser01_07_gold_trends_and_watchlist` | `07_gold_trends_and_watchlist.py` |

Upload **all** `.py` files from `scripts/` to a **Files** resource per participant (e.g. `holuser01-insurance`).

**Before DQ labs:** one **Python Environment** resource per participant (or shared) built from `scripts/requirements.txt` — see `docs/CDE_PYTHON_ENVIRONMENT.md`.

Airflow: `medallion_airflow_dag.py` (job names must match exactly).

---

## Catalog objects (holuser01_insurance_analytics)

**Bronze:** `customers`, `claims`  
**Silver:** `silver_customers`, `silver_claims`, `silver_claims_enriched`  
**Gold:** `gold_claims_kpi_by_state`, `gold_monthly_claim_trends`, `gold_high_risk_watchlist`  
**CDW views:** `vw_executive_claims_by_state`, `vw_high_risk_claims_report`

---

## Ranger / permissions

- S3 read: `cloudera-hol-buk-99feb843/data/user/holuser01/*`
- SQL: SELECT on database above; CREATE VIEW for CDW step
- CAI: Spark/Hive access to gold tables
- Agent Studio: read-only SQL tool as workshop user

---

## Regenerate local data (if needed)

```bash
cd insurance_claims/codes && source ../../.venv/bin/activate
python generate_data.py 100000 customers ../data/customers.csv
python generate_data.py 100000000 claims ../data/claims.parquet
```
