#
# Medallion — Silver claims + UDFs registered for Spark SQL.
#
# Libraries: holidays (US federal calendar), python-dateutil (relativedelta).
# Compare with job 03 (scipy via DataFrame pandas UDF).
#
# Python Environment: see cde/requirements.txt

from datetime import datetime

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType
import holidays

from workshop_config import insurance_db, log_identity, resolve_username

# Workshop book-end for tenure-style month counts (matches generator END_DATE).
BOOK_END = datetime(2024, 12, 31)
US_HOLIDAYS = holidays.US()


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


def _as_naive_datetime(value):
    if value is None:
        return None
    if hasattr(value, "toPydatetime"):
        value = value.toPydatetime()
    if getattr(value, "tzinfo", None) is not None:
        value = value.replace(tzinfo=None)
    return value


def is_us_federal_holiday_py(claim_ts):
    """True when claim_date falls on a US federal holiday (holidays library)."""
    dt = _as_naive_datetime(claim_ts)
    if dt is None:
        return None
    return dt.date() in US_HOLIDAYS


def months_to_book_end_py(claim_ts):
    """Whole months from claim_date to BOOK_END using relativedelta (dateutil)."""
    dt = _as_naive_datetime(claim_ts)
    if dt is None:
        return None
    delta = relativedelta(BOOK_END, dt)
    return delta.years * 12 + delta.months


is_us_federal_holiday_udf = udf(is_us_federal_holiday_py, BooleanType())
months_to_book_end_udf = udf(months_to_book_end_py, IntegerType())

spark = build_spark("CDE-silver-claims")
log_identity(spark)
username = resolve_username(spark)
db_name = insurance_db(spark)

print("Register UDFs for Spark SQL: holidays + dateutil.relativedelta")
spark.udf.register("is_us_federal_holiday", is_us_federal_holiday_udf)
spark.udf.register("months_to_book_end", months_to_book_end_udf)

print("...............................")
print("Silver: SQL CTAS invoking registered UDFs")
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
      is_us_federal_holiday(claim_date) AS is_us_federal_holiday,
      months_to_book_end(claim_date) AS months_to_book_end,
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
    f"""
    SELECT
      SUM(CASE WHEN is_us_federal_holiday THEN 1 ELSE 0 END) AS holiday_claims,
      ROUND(AVG(months_to_book_end), 2) AS avg_months_to_book_end
    FROM {db_name}.silver_claims
    """
).show()

spark.sql(
    f"""
    SELECT claim_date, is_us_federal_holiday, months_to_book_end, claim_amount
    FROM {db_name}.silver_claims
    WHERE is_us_federal_holiday = true
    LIMIT 5
    """
).show(truncate=False)

print("Job 04 complete.")
