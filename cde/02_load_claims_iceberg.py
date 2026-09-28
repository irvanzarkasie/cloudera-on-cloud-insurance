#
# Copyright (c) 2026 Cloudera, Inc. All rights reserved.
#
# CDE Job 2 (Bronze): Load insurance claims Parquet into Iceberg table `claims`.

from pyspark.sql import SparkSession

from workshop_config import insurance_db, log_identity, resolve_username, workshop_data_base


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

# Full workshop dataset (~100M rows). For a smoke test, use claims_1m_sample.parquet in the same folder.
spark = build_spark("CDE-insurance-load-claims")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)
claims_path = f"{workshop_data_base(spark)}/claims.parquet"

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
