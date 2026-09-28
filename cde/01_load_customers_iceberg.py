#
# Copyright (c) 2026 Cloudera, Inc. All rights reserved.
#
# CDE Job 1 (Bronze): Load insurance customers CSV into Iceberg table `customers`.

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

db_name = username.replace("-", "_") + "_insurance_analytics"
app_name = f"{username}-CDE-insurance-load-customers"

customers_path = f"{data_base}/customers.csv"

spark = build_spark(app_name)

print("...............................")
print(f"Reading customers from {customers_path}")
customers_df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(customers_path)
)

customers_df.printSchema()
print("Sample rows:")
customers_df.show(5, truncate=False)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

target = f"{db_name}.customers"
print("...............................")
print(f"Writing Iceberg table {target}")
customers_df.writeTo(target).using("iceberg").createOrReplace()

spark.sql(f"SELECT COUNT(*) AS customer_count FROM {target}").show()
spark.sql(f"SELECT * FROM {target} LIMIT 15").show()

print("Job 1 complete.")
