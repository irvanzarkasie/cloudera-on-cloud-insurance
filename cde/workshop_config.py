#
# Shared workshop identity for CDE jobs (upload with all job scripts in the Resource).
#
# Resolution order:
#   1. spark.sparkContext.sparkUser() (CDE --proxy-user / workload user)
#   2. WORKSHOP_USER, HADOOP_USER_NAME, SPARK_USER, CDE_USER, USER, LOGNAME

import os

WORKSHOP_BUCKET = "cloudera-hol-buk-99feb843"
_FALLBACK_USER = "holuser01"
_IGNORE_USERS = frozenset({"root", "yarn", "hdfs", "spark", "airflow"})


def resolve_runtime_user(spark=None) -> str:
    """CDP/CDE login id as-is (use for S3 path segments)."""
    if spark is not None:
        try:
            user = spark.sparkContext.sparkUser()
            if user and user.strip().lower() not in _IGNORE_USERS:
                return user.strip()
        except Exception:
            pass
    for key in (
        "WORKSHOP_USER",
        "HADOOP_USER_NAME",
        "SPARK_USER",
        "CDE_USER",
        "USER",
        "LOGNAME",
    ):
        val = os.environ.get(key)
        if val and val.strip() and val.strip().lower() not in _IGNORE_USERS:
            return val.strip()
    return _FALLBACK_USER


def resolve_username(spark=None) -> str:
    """Hive / job-name safe prefix (hyphens → underscores)."""
    return resolve_runtime_user(spark).replace("-", "_")


def insurance_db(spark=None) -> str:
    return f"{resolve_username(spark)}_insurance_analytics"


def workshop_data_base(spark=None, bucket: str = WORKSHOP_BUCKET) -> str:
    return f"s3a://{bucket}/data/user/{resolve_runtime_user(spark)}"


def log_identity(spark, label: str = "Workshop identity") -> None:
    print(
        f"{label}: runtime_user={resolve_runtime_user(spark)!r}, "
        f"username={resolve_username(spark)!r}, db={insurance_db(spark)!r}, "
        f"data_base={workshop_data_base(spark)!r}"
    )
