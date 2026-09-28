#
# Medallion — Data quality gate: Great Expectations on silver Iceberg table.
#
# CDE (recommended): attach Python Environment resource — docs/CDE_PYTHON_ENVIRONMENT.md
#
# Execution order:
#   1. Create Spark session
#   2. Load Iceberg table as Spark DataFrame
#   3. Run Great Expectations suite on the DataFrame

import subprocess
import sys

from pyspark.sql import SparkSession

GX_VERSION = "0.18.22"


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

username = "holuser01"
db_name = "holuser01_insurance_analytics"
# Table to validate (after job 05; gate before gold jobs)
source_table = f"{db_name}.silver_claims_enriched"

VALID_STATES = ("CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI")


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


def run_expectations(spark_df):
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

    failed = []
    for label, result in checks:
        status = "PASS" if result["success"] else "FAIL"
        print(f"  [{status}] {label}")
        if not result["success"]:
            failed.append(label)
            unexpected = result.get("result", {}).get("partial_unexpected_list", [])
            if unexpected:
                print(f"         sample unexpected: {unexpected[:5]}")

    return failed


print("Step 1: Create Spark session")
spark = build_spark(f"{username}-CDE-data-quality-gx")

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
failures = run_expectations(df)

print("...............................")
if failures:
    print(f"DATA QUALITY FAILED ({len(failures)} checks): {failures}")
    sys.exit(1)

print("DATA QUALITY PASSED — all expectations succeeded.")
print("Job 08 complete.")
