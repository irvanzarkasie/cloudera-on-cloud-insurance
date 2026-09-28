#
# Medallion — Data quality gate: Great Expectations on silver_claims_enriched.
#
# CDE (recommended): attach Python Environment resource — docs/CDE_PYTHON_ENVIRONMENT.md
#
#   1. Create Spark session
#   2. Load Iceberg table as Spark DataFrame
#   3. Run Great Expectations suite on the DataFrame
#   4. Append one row per check to Iceberg data_quality_check (same table as job 09;
#      keyed by execution_id + target_table)

import subprocess
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

GX_VERSION = "0.18.22"

from workshop_config import insurance_db, log_identity, resolve_username

VALID_STATES = ("CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI")

DQ_RESULTS_SCHEMA = T.StructType(
    [
        T.StructField("execution_id", T.StringType(), False),
        T.StructField("execution_time", T.TimestampType(), False),
        T.StructField("target_table", T.StringType(), False),
        T.StructField("check_name", T.StringType(), False),
        T.StructField("dq_score", T.DoubleType(), False),
        T.StructField("success", T.BooleanType(), False),
        T.StructField("element_count", T.LongType(), True),
        T.StructField("unexpected_count", T.LongType(), True),
    ]
)


def ensure_great_expectations():
    try:
        import great_expectations  # noqa: F401
        return
    except ModuleNotFoundError:
        print(
            f"great_expectations not on driver; pip install great_expectations=={GX_VERSION} ..."
        )
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                f"great_expectations=={GX_VERSION}",
                "--quiet",
            ]
        )


def build_spark(app_name):
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .enableHiveSupport()
        .getOrCreate()
    )


def dq_score_from_result(result):
    if result.get("success"):
        return 1.0
    r = result.get("result") or {}
    element_count = r.get("element_count")
    unexpected_count = r.get("unexpected_count")
    if element_count and element_count > 0 and unexpected_count is not None:
        return max(0.0, 1.0 - (unexpected_count / element_count))
    return 0.0


def run_expectations(spark_df, execution_id, execution_time):
    from great_expectations.dataset import SparkDFDataset

    ge_df = SparkDFDataset(spark_df, batch_kwargs={"data_asset_name": source_table})

    checks = [
        ("claim_id not null", ge_df.expect_column_values_to_not_be_null("claim_id")),
        ("customer_id not null", ge_df.expect_column_values_to_not_be_null("customer_id")),
        (
            "claim_amount positive",
            ge_df.expect_column_values_to_be_between(
                "claim_amount", min_value=0, strict_min=True
            ),
        ),
        (
            "fraud_risk_score in [0,1]",
            ge_df.expect_column_values_to_be_between(
                "fraud_risk_score", min_value=0, max_value=1
            ),
        ),
        (
            "state valid",
            ge_df.expect_column_values_to_be_in_set("state", value_set=list(VALID_STATES)),
        ),
        (
            "fraud_risk_band valid",
            ge_df.expect_column_values_to_be_in_set(
                "fraud_risk_band", value_set=["LOW", "MEDIUM", "HIGH"]
            ),
        ),
        (
            "claim_id mostly unique",
            ge_df.expect_column_proportion_of_unique_values_to_be_between(
                "claim_id", min_value=0.999
            ),
        ),
    ]

    rows = []
    failed = []
    for label, result in checks:
        success = bool(result["success"])
        score = dq_score_from_result(result)
        r = result.get("result") or {}
        status = "PASS" if success else "FAIL"
        print(f"  [{status}] {label} (dq_score={score:.4f})")
        if not success:
            failed.append(label)
            unexpected = r.get("partial_unexpected_list", [])
            if unexpected:
                print(f"         sample unexpected: {unexpected[:5]}")

        rows.append(
            {
                "execution_id": execution_id,
                "execution_time": execution_time,
                "target_table": source_table,
                "check_name": label,
                "dq_score": score,
                "success": success,
                "element_count": r.get("element_count"),
                "unexpected_count": r.get("unexpected_count"),
            }
        )

    return failed, rows


def persist_dq_results(spark, rows):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    results_df = spark.createDataFrame(rows, schema=DQ_RESULTS_SCHEMA)
    if spark.catalog.tableExists(dq_results_table):
        results_df.writeTo(dq_results_table).using("iceberg").append()
    else:
        results_df.writeTo(dq_results_table).using("iceberg").create()
    print(f"Wrote {len(rows)} DQ metric rows to {dq_results_table}")


print("Step 1: Create Spark session")
spark = build_spark("CDE-data-quality-gx")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)
source_table = f"{db_name}.silver_claims_enriched"
dq_results_table = f"{db_name}.data_quality_check"

execution_id = str(uuid.uuid4())
execution_time = datetime.now(timezone.utc).replace(tzinfo=None)
print(f"execution_id={execution_id}")
print(f"execution_time={execution_time} UTC")
print(f"target_table={source_table}")

print("...............................")
print(f"Step 2: Query Iceberg table {source_table}")
df = spark.table(source_table)
row_count = df.count()
print(f"Row count: {row_count}")
if row_count == 0:
    print("ERROR: source table is empty")
    sys.exit(1)

df.printSchema()
df.show(3, truncate=False)

print("...............................")
print("Step 3: Run Great Expectations test suite")
ensure_great_expectations()
failures, dq_rows = run_expectations(df, execution_id, execution_time)

print("...............................")
print(f"Step 4: Persist results to {dq_results_table}")
persist_dq_results(spark, dq_rows)

spark.sql(
    f"""
    SELECT execution_id, target_table, check_name, dq_score, success
    FROM {dq_results_table}
    WHERE execution_id = '{execution_id}'
    ORDER BY check_name
    """
).show(truncate=False)

print("...............................")
if failures:
    print(f"DATA QUALITY FAILED ({len(failures)} checks): {failures}")
    sys.exit(1)

print("DATA QUALITY PASSED — all expectations succeeded.")
print("Job 08 complete.")
