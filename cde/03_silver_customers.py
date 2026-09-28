#
# Medallion — Silver layer (Job 1 of 5): cleanse customer dimension from bronze.

from pyspark.sql import SparkSession

username = "holuser01"
db_name = "holuser01_insurance_analytics"


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


spark = build_spark(f"{username}-CDE-silver-customers")

print("Silver: customers from bronze.customers")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {db_name}.silver_customers
    USING iceberg
    AS
    SELECT
      customer_id,
      CAST(age AS INT) AS age,
      UPPER(TRIM(state)) AS state,
      CAST(fraud_risk_score AS DOUBLE) AS fraud_risk_score,
      CASE
        WHEN fraud_risk_score >= 0.85 THEN 'HIGH'
        WHEN fraud_risk_score >= 0.60 THEN 'MEDIUM'
        ELSE 'LOW'
      END AS fraud_risk_band,
      current_timestamp() AS silver_processed_at
    FROM {db_name}.customers
    WHERE customer_id IS NOT NULL
      AND age BETWEEN 18 AND 100
      AND state IS NOT NULL
    """
)

spark.sql(f"SELECT COUNT(*) AS silver_customer_rows FROM {db_name}.silver_customers").show()
spark.sql(
    f"SELECT fraud_risk_band, COUNT(*) AS cnt FROM {db_name}.silver_customers "
    "GROUP BY fraud_risk_band ORDER BY cnt DESC"
).show()

print("Job 03 complete.")
