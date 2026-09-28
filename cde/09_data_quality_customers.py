#
# Sample data quality job: Great Expectations on bronze Iceberg table `customers`.
#
# CDE (recommended): create Resource type "Python Environment" with requirements.txt,
#      then Job → Configurations → Python Environment → select that resource.
#      See docs/CDE_PYTHON_ENVIRONMENT.md
#
# Suggested run order: after job 01 (load customers), before silver jobs.
#
#   1. Create Spark session
#   2. Query customers Iceberg table as Spark DataFrame
#   3. Execute Great Expectations test suite on the DataFrame

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
source_table = f"{db_name}.customers"

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
        ("customer_id not null", ge_df.expect_column_values_to_not_be_null("customer_id")),
        ("customer_id unique", ge_df.expect_column_values_to_be_unique("customer_id")),
        (
            "age in range",
            ge_df.expect_column_values_to_be_between("age", min_value=18, max_value=100),
        ),
        (
            "state valid",
            ge_df.expect_column_values_to_be_in_set("state", value_set=list(VALID_STATES)),
        ),
        (
            "fraud_risk_score in [0,1]",
            ge_df.expect_column_values_to_be_between(
                "fraud_risk_score", min_value=0, max_value=1
            ),
        ),
        (
            "row count ~100k",
            ge_df.expect_table_row_count_to_be_between(min_value=95_000, max_value=105_000),
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
spark = build_spark(f"{username}-CDE-data-quality-customers")

print("...............................")
print(f"Step 2: Query Iceberg table {source_table}")
df = spark.table(source_table)
row_count = df.count()
print(f"Row count: {row_count}")
if row_count == 0:
    print("ERROR: customers table is empty")
    sys.exit(1)

df.printSchema()
df.show(5, truncate=False)

print("...............................")
print("Step 3: Run Great Expectations test suite")
ensure_great_expectations()
failures = run_expectations(df)

print("...............................")
if failures:
    print(f"DATA QUALITY FAILED ({len(failures)} checks): {failures}")
    sys.exit(1)

print("DATA QUALITY PASSED — all customer expectations succeeded.")
print("Job 09 complete.")
