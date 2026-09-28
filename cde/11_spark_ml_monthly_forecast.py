#
# Spark MLlib: forecast monthly claim_count and total_claim_amount from gold trends.
#
# Run after job 07 (gold_monthly_claim_trends). Time-based holdout — better fit than
# row-level denial prediction on random synthetic statuses.
#
# ponytail: small row count (~months × policy types); GBT on time index + product line.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max, month, year
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor

from workshop_config import insurance_db, log_identity, resolve_username

HOLDOUT_MONTHS = 3
RANDOM_SEED = 42


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


def month_ordinal_col():
    return (year(col("claim_month")) - 2022) * 12 + month(col("claim_month"))


def regression_pipeline(label_col):
    policy_idx = StringIndexer(
        inputCol="policy_type", outputCol="policy_idx", handleInvalid="keep"
    )
    assembler = VectorAssembler(
        inputCols=["month_ordinal", "policy_idx"], outputCol="features"
    )
    regressor = GBTRegressor(
        labelCol=label_col,
        featuresCol="features",
        maxIter=50,
        maxDepth=4,
        seed=RANDOM_SEED,
    )
    return Pipeline(stages=[policy_idx, assembler, regressor])


spark = build_spark("CDE-spark-ml-monthly-forecast")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)
source_table = f"{db_name}.gold_monthly_claim_trends"
output_table = f"{db_name}.gold_ml_monthly_forecast"

print(f"Load {source_table}")
trends = (
    spark.table(source_table)
    .select(
        "claim_month",
        "policy_type",
        col("claim_count").cast("double"),
        col("total_claim_amount").cast("double"),
    )
    .withColumn("month_ordinal", month_ordinal_col())
)

row_n = trends.count()
print(f"Trend rows (month × policy_type): {row_n}")
if row_n < 12:
    print("ERROR: not enough history — run job 07 first")
    raise SystemExit(1)

distinct_months = [
    r.claim_month
    for r in trends.select("claim_month").distinct().orderBy("claim_month").collect()
]
holdout = set(distinct_months[-HOLDOUT_MONTHS:])
print(f"Holdout months ({HOLDOUT_MONTHS}): {sorted(holdout)}")

train = trends.filter(~col("claim_month").isin(holdout))
test = trends.filter(col("claim_month").isin(holdout))

# --- claim_count model ---
print("...............................")
print("Train GBTRegressor — claim_count")
count_model = regression_pipeline("claim_count").fit(train)
count_test = count_model.transform(test)
count_rmse = RegressionEvaluator(
    labelCol="claim_count", predictionCol="prediction", metricName="rmse"
).evaluate(count_test)
count_mae = RegressionEvaluator(
    labelCol="claim_count", predictionCol="prediction", metricName="mae"
).evaluate(count_test)
print(f"claim_count — holdout RMSE: {count_rmse:,.2f}, MAE: {count_mae:,.2f}")

# --- total_claim_amount model ---
print("Train GBTRegressor — total_claim_amount")
amount_model = regression_pipeline("total_claim_amount").fit(train)
amount_test = amount_model.transform(test)
amount_rmse = RegressionEvaluator(
    labelCol="total_claim_amount", predictionCol="prediction", metricName="rmse"
).evaluate(amount_test)
amount_mae = RegressionEvaluator(
    labelCol="total_claim_amount", predictionCol="prediction", metricName="mae"
).evaluate(amount_test)
print(f"total_claim_amount — holdout RMSE: {amount_rmse:,.2f}, MAE: {amount_mae:,.2f}")

# --- forward: one step after latest month per policy_type ---
max_ord = trends.agg(spark_max("month_ordinal")).collect()[0][0]
policy_types = [r.policy_type for r in trends.select("policy_type").distinct().collect()]
forward_rows = [
    (None, pt, float(max_ord + 1)) for pt in policy_types
]
forward = spark.createDataFrame(
    forward_rows, "claim_month timestamp, policy_type string, month_ordinal double"
)

count_fwd = count_model.transform(forward).select(
    lit(None).cast("timestamp").alias("claim_month"),
    col("policy_type"),
    lit("claim_count").alias("metric"),
    lit(None).cast("double").alias("actual_value"),
    col("prediction").alias("predicted_value"),
    lit("forward").alias("forecast_type"),
)

amount_fwd = amount_model.transform(forward).select(
    lit(None).cast("timestamp").alias("claim_month"),
    col("policy_type"),
    lit("total_claim_amount").alias("metric"),
    lit(None).cast("double").alias("actual_value"),
    col("prediction").alias("predicted_value"),
    lit("forward").alias("forecast_type"),
)

def holdout_rows(model, label, metric_name):
    pred = model.transform(test)
    return pred.select(
        col("claim_month"),
        col("policy_type"),
        lit(metric_name).alias("metric"),
        col(label).alias("actual_value"),
        col("prediction").alias("predicted_value"),
        lit("holdout").alias("forecast_type"),
    )


holdout_out = holdout_rows(count_model, "claim_count", "claim_count").unionByName(
    holdout_rows(amount_model, "total_claim_amount", "total_claim_amount")
)

out = holdout_out.unionByName(count_fwd).unionByName(amount_fwd)

print("...............................")
print(f"Write forecasts to {output_table}")
out.writeTo(output_table).using("iceberg").createOrReplace()

spark.sql(
    f"""
    SELECT forecast_type, metric, policy_type,
           ROUND(AVG(actual_value), 2) AS avg_actual,
           ROUND(AVG(predicted_value), 2) AS avg_predicted
    FROM {output_table}
    GROUP BY forecast_type, metric, policy_type
    ORDER BY forecast_type, metric, policy_type
    """
).show(truncate=False)

print("Job 11 complete.")
