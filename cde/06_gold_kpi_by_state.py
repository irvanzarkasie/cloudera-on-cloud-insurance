#
# Medallion — Gold layer (Job 4 of 5): executive KPIs by state and policy type.

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


spark = build_spark(f"{username}-CDE-gold-kpi-by-state")

print("Gold: KPI summary by state and policy type")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {db_name}.gold_claims_kpi_by_state
    USING iceberg
    AS
    SELECT
      state,
      policy_type,
      COUNT(*) AS claim_count,
      ROUND(SUM(claim_amount), 2) AS total_claim_amount,
      ROUND(AVG(claim_amount), 2) AS avg_claim_amount,
      ROUND(AVG(fraud_risk_score), 4) AS avg_fraud_risk_score,
      SUM(CASE WHEN fraud_risk_band = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_customer_claims,
      SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) AS denied_claims,
      current_timestamp() AS gold_built_at
    FROM {db_name}.silver_claims_enriched
    GROUP BY state, policy_type
    """
)

spark.sql(
    f"SELECT * FROM {db_name}.gold_claims_kpi_by_state "
    "ORDER BY total_claim_amount DESC LIMIT 10"
).show()

print("Job 06 complete.")
