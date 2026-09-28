#
# Copyright (c) 2026 Cloudera, Inc. All rights reserved.
#
# CDE Job 2 (Bronze): Load insurance claims Parquet into Iceberg table `claims`.

from pyspark.sql import SparkSession


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

# --- Workshop settings ---
username = "holuser01".replace("-", "_")
data_base = "s3a://cloudera-hol-buk-99feb843/data/user/holuser01"

# Full workshop dataset (~100M rows). For a smoke test, use claims_1m_sample.parquet in the same folder.
claims_path = f"{data_base}/claims.parquet"

db_name = username.replace("-", "_") + "_insurance_analytics"
app_name = f"{username}-CDE-insurance-load-claims"

spark = build_spark(app_name)

print("...............................")
print(f"Reading claims from {claims_path}")
claims_df = spark.read.parquet(claims_path)

claims_df.printSchema()
print("Sample rows:")
claims_df.show(5, truncate=False)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

target = f"{db_name}.claims"
print("...............................")
print(f"Writing Iceberg table {target}")
claims_df.writeTo(target).using("iceberg").createOrReplace()

spark.sql(f"SELECT COUNT(*) AS claim_count FROM {target}").show()
spark.sql(
    f"SELECT policy_type, COUNT(*) AS cnt FROM {target} GROUP BY policy_type ORDER BY cnt DESC"
).show()

print("Job 2 complete.")
