#
# Medallion — Gold layer (Job 5 of 5): monthly trends + high-risk watchlist.

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


spark = build_spark("CDE-gold-trends-watchlist")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)

print("Gold: monthly claim trends")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {db_name}.gold_monthly_claim_trends
    USING iceberg
    AS
    SELECT
      claim_month,
      policy_type,
      COUNT(*) AS claim_count,
      ROUND(SUM(claim_amount), 2) AS total_claim_amount,
      ROUND(AVG(claim_amount), 2) AS avg_claim_amount,
      current_timestamp() AS gold_built_at
    FROM {db_name}.silver_claims_enriched
    GROUP BY claim_month, policy_type
    """
)

print("Gold: high-risk watchlist for fraud ops")
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {db_name}.gold_high_risk_watchlist
    USING iceberg
    AS
    SELECT
      claim_id,
      customer_id,
      state,
      policy_type,
      claim_type,
      claim_amount,
      claim_status,
      fraud_risk_score,
      fraud_risk_band,
      claim_date
    FROM {db_name}.silver_claims_enriched
    WHERE fraud_risk_band = 'HIGH'
      AND claim_amount >= 50000
      AND claim_status IN ('APPROVED', 'PENDING', 'IN REVIEW')
    """
)

for t in ("gold_monthly_claim_trends", "gold_high_risk_watchlist"):
    spark.sql(f"SELECT COUNT(*) AS cnt FROM {db_name}.{t}").show()

print("Job 07 complete.")
