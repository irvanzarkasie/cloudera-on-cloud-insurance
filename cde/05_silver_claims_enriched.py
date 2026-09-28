#
# Medallion — Silver layer (Job 3 of 5): join cleansed claims + customers.

from pyspark.sql import SparkSession

from workshop_config import insurance_db, log_identity, resolve_username


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


spark = build_spark("CDE-silver-claims-enriched")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)

print("Silver: enriched claims (join silver_claims + silver_customers)")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {db_name}.silver_claims_enriched
    USING iceberg
    AS
    SELECT
      cl.claim_id,
      cl.policy_id,
      cl.customer_id,
      cl.claim_date,
      cl.claim_month,
      cl.claim_amount,
      cl.claim_status,
      cl.policy_type,
      cl.claim_type,
      cu.age,
      cu.state,
      cu.fraud_risk_score,
      cu.fraud_risk_band,
      current_timestamp() AS enriched_at
    FROM {db_name}.silver_claims cl
    INNER JOIN {db_name}.silver_customers cu
      ON cl.customer_id = cu.customer_id
    """
)

spark.sql(
    f"SELECT COUNT(*) AS enriched_rows FROM {db_name}.silver_claims_enriched"
).show()
spark.sql(
    f"SELECT state, COUNT(*) AS claims FROM {db_name}.silver_claims_enriched "
    "GROUP BY state ORDER BY claims DESC LIMIT 5"
).show()

print("Job 05 complete.")
