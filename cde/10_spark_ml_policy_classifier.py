#
# Spark MLlib: (1) policy_type multiclass + (2) claim denied vs not denied (binary).
#
# Run after job 05 (silver_claims_enriched). pyspark.ml only — no extra pip deps.
#
# ponytail: TRAIN_FRACTION caps rows for workshop runtime; denial label is weak in synthetic data.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import BooleanType, DoubleType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import StringIndexer, VectorAssembler

from workshop_config import insurance_db, log_identity, resolve_username

TRAIN_FRACTION = 0.1
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


def feature_pipeline_stages():
    """Shared categoricals + numeric features (policy_type is not included — separate models)."""
    return [
        StringIndexer(inputCol="state", outputCol="state_idx", handleInvalid="keep"),
        StringIndexer(
            inputCol="claim_type", outputCol="claim_type_idx", handleInvalid="keep"
        ),
        VectorAssembler(
            inputCols=[
                "claim_amount",
                "age",
                "fraud_risk_score",
                "state_idx",
                "claim_type_idx",
            ],
            outputCol="features",
        ),
    ]


spark = build_spark("CDE-spark-ml-claims")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)
source_table = f"{db_name}.silver_claims_enriched"
predictions_table = f"{db_name}.gold_ml_claim_predictions"

print(f"Load data from {source_table}")
raw = (
    spark.table(source_table)
    .select(
        "claim_id",
        "claim_amount",
        "claim_type",
        "age",
        "state",
        "fraud_risk_score",
        "policy_type",
        "claim_status",
    )
    .dropna(subset=["policy_type", "claim_amount", "claim_type", "state", "claim_status"])
    .withColumn(
        "is_denied",
        when(col("claim_status") == "DENIED", 1.0).otherwise(0.0),
    )
)

sample = raw.sample(withReplacement=False, fraction=TRAIN_FRACTION, seed=RANDOM_SEED)
sample.cache()
n = sample.count()
print(f"Sample row count: {n}")
if n < 1000:
    print("ERROR: sample too small — run job 05 first or increase TRAIN_FRACTION")
    raise SystemExit(1)

train, test = sample.randomSplit([0.8, 0.2], seed=RANDOM_SEED)
print(f"Train: {train.count()}, Test: {test.count()}")

# --- Model 1: policy_type (multiclass) ---
policy_label = StringIndexer(
    inputCol="policy_type", outputCol="policy_label", handleInvalid="skip"
)
policy_clf = RandomForestClassifier(
    labelCol="policy_label",
    featuresCol="features",
    numTrees=50,
    maxDepth=8,
    seed=RANDOM_SEED,
)
policy_pipeline = Pipeline(
    stages=[policy_label] + feature_pipeline_stages() + [policy_clf]
)

print("...............................")
print("Fit model 1: RandomForest — policy_type")
policy_model = policy_pipeline.fit(train)

policy_pred = policy_model.transform(test)
policy_accuracy = MulticlassClassificationEvaluator(
    labelCol="policy_label", predictionCol="prediction", metricName="accuracy"
).evaluate(policy_pred)
policy_f1 = MulticlassClassificationEvaluator(
    labelCol="policy_label", predictionCol="prediction", metricName="f1"
).evaluate(policy_pred)
print(f"Policy type — test accuracy: {policy_accuracy:.4f}, F1: {policy_f1:.4f}")

policy_labels = policy_model.stages[0].labels

# --- Model 2: denied vs accepted (binary; "accepted" = not DENIED) ---
denial_clf = RandomForestClassifier(
    labelCol="is_denied",
    featuresCol="features",
    numTrees=50,
    maxDepth=8,
    seed=RANDOM_SEED,
)
denial_pipeline = Pipeline(stages=feature_pipeline_stages() + [denial_clf])

print("...............................")
print("Fit model 2: RandomForest — is_denied (1=DENIED, 0=otherwise)")
denial_model = denial_pipeline.fit(train)

denial_pred = denial_model.transform(test)
denial_auc = BinaryClassificationEvaluator(
    labelCol="is_denied",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC",
).evaluate(denial_pred)
denial_accuracy = MulticlassClassificationEvaluator(
    labelCol="is_denied", predictionCol="prediction", metricName="accuracy"
).evaluate(denial_pred)
print(f"Denial — test accuracy: {denial_accuracy:.4f}, AUC: {denial_auc:.4f}")
print(
    "(Synthetic data: claim_status is random — expect ~chance performance; "
    "real data would show stronger signal.)"
)


@udf(StringType())
def idx_to_policy(idx):
    if idx is None:
        return None
    i = int(idx)
    return policy_labels[i] if 0 <= i < len(policy_labels) else None


@udf(BooleanType())
def idx_to_denied(idx):
    if idx is None:
        return None
    return int(idx) == 1


@udf(DoubleType())
def prob_denied(prob_vec):
    if prob_vec is None:
        return None
    return float(prob_vec[1])


policy_out = policy_pred.select(
    "claim_id",
    col("policy_type").alias("actual_policy_type"),
    idx_to_policy(col("prediction").cast("double")).alias("predicted_policy_type"),
)

denial_out = denial_pred.select(
    "claim_id",
    (col("is_denied") == 1.0).alias("actual_denied"),
    idx_to_denied(col("prediction").cast("double")).alias("predicted_denied"),
    prob_denied(col("probability")).alias("predicted_denied_probability"),
)

print("...............................")
print(f"Write combined predictions to {predictions_table}")
out = policy_out.join(denial_out, "claim_id")
out.writeTo(predictions_table).using("iceberg").createOrReplace()

spark.sql(f"SELECT COUNT(*) FROM {predictions_table}").show()

print("Policy type confusion (top pairs):")
spark.sql(
    f"""
    SELECT actual_policy_type, predicted_policy_type, COUNT(*) AS cnt
    FROM {predictions_table}
    GROUP BY actual_policy_type, predicted_policy_type
    ORDER BY cnt DESC
    LIMIT 8
    """
).show()

print("Denial vs predicted denial:")
spark.sql(
    f"""
    SELECT actual_denied, predicted_denied, COUNT(*) AS cnt
    FROM {predictions_table}
    GROUP BY actual_denied, predicted_denied
    ORDER BY cnt DESC
    """
).show()

sample.unpersist()
print("Job 10 complete.")
