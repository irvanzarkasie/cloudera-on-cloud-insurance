#
# Medallion — Silver customers + PySpark UDF (DataFrame API) using scipy.stats.
#
# Python Environment: scipy, pyarrow, pandas (pandas_udf needs PyArrow >= 4) — see requirements.txt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, upper, when
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import pandas_udf

import pandas as pd
from scipy.stats import rankdata, zscore

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


@pandas_udf(DoubleType())
def fraud_risk_zscore_udf(scores: pd.Series) -> pd.Series:
    """Batch z-score via scipy (mean/std normalisation per Spark partition batch)."""
    vals = scores.astype(float)
    z = zscore(vals, nan_policy="omit")
    return pd.Series(z)


@pandas_udf(DoubleType())
def fraud_risk_percentile_udf(scores: pd.Series) -> pd.Series:
    """Percentile rank 0–100 via scipy.stats.rankdata (tie-aware)."""
    vals = scores.astype(float)
    ranks = rankdata(vals, method="average")
    return pd.Series(ranks / len(ranks) * 100.0)


spark = build_spark("CDE-silver-customers")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)

print("Silver: read bronze.customers")
bronze = spark.table(f"{db_name}.customers")

base = (
    bronze.filter(col("customer_id").isNotNull() & col("state").isNotNull())
    .filter((col("age") >= 18) & (col("age") <= 100))
    .withColumn("age", col("age").cast("int"))
    .withColumn("state", upper(trim(col("state"))))
    .withColumn("fraud_risk_score", col("fraud_risk_score").cast("double"))
    .withColumn(
        "fraud_risk_band",
        when(col("fraud_risk_score") >= 0.85, "HIGH")
        .when(col("fraud_risk_score") >= 0.60, "MEDIUM")
        .otherwise("LOW"),
    )
)

print("...............................")
print("DataFrame API: pandas UDFs backed by scipy.stats")
silver = (
    base.withColumn("fraud_risk_zscore", fraud_risk_zscore_udf(col("fraud_risk_score")))
    .withColumn("fraud_risk_percentile", fraud_risk_percentile_udf(col("fraud_risk_score")))
    .withColumn("silver_processed_at", current_timestamp())
    .select(
        "customer_id",
        "age",
        "state",
        "fraud_risk_score",
        "fraud_risk_zscore",
        "fraud_risk_percentile",
        "fraud_risk_band",
        "silver_processed_at",
    )
)

print("Sample rows after scipy UDF enrichment:")
silver.show(5, truncate=False)

target = f"{db_name}.silver_customers"
print(f"Write Iceberg table {target}")
silver.writeTo(target).using("iceberg").createOrReplace()

spark.sql(f"SELECT COUNT(*) AS silver_customer_rows FROM {target}").show()
spark.sql(
    f"SELECT fraud_risk_band, ROUND(AVG(fraud_risk_percentile), 2) AS avg_pct "
    f"FROM {target} GROUP BY fraud_risk_band ORDER BY avg_pct DESC"
).show()

print("Job 03 complete.")
