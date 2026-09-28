#
# Medallion — Silver layer (Job 2 of 5): cleanse claims fact from bronze.

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


spark = build_spark(f"{username}-CDE-silver-claims")

print("Silver: claims from bronze.claims")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {db_name}.silver_claims
    USING iceberg
    AS
    SELECT
      claim_id,
      policy_id,
      customer_id,
      claim_date,
      CAST(claim_amount AS DOUBLE) AS claim_amount,
      UPPER(TRIM(claim_status)) AS claim_status,
      policy_type,
      claim_type,
      date_trunc('MONTH', claim_date) AS claim_month,
      current_timestamp() AS silver_processed_at
    FROM {db_name}.claims
    WHERE claim_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND claim_date IS NOT NULL
      AND claim_amount > 0
      AND claim_status IN ('Approved', 'Pending', 'Denied', 'In Review')
    """
)

spark.sql(f"SELECT COUNT(*) AS silver_claim_rows FROM {db_name}.silver_claims").show()
spark.sql(
    f"SELECT policy_type, COUNT(*) AS cnt FROM {db_name}.silver_claims "
    "GROUP BY policy_type ORDER BY cnt DESC"
).show()

print("Job 04 complete.")
