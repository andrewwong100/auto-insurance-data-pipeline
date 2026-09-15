import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    desc,
    input_file_name,
    length,
    lit,
    row_number,
    to_date,
    to_timestamp,
    trim,
    upper,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType


# ------------------------------------------------------------------
# Job initialization
# ------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "PROCESS_DATE",
    ],
)

process_date = args["PROCESS_DATE"]

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


# ------------------------------------------------------------------
# S3 paths
# ------------------------------------------------------------------

input_path = (
    "s3://bronze-auto-insurance/claims-events/"
    f"claim_events_{process_date}.csv"
)

silver_path = (
    "s3://silver-auto-insurance/claims_events/"
    f"load_date={process_date}/"
)

quarantine_path = (
    "s3://silver-auto-insurance/quarantine/claims_events/"
    f"load_date={process_date}/"
)


# ------------------------------------------------------------------
# Bronze schema
#
# All Bronze columns are initially strings. This prevents malformed
# values from silently disappearing during CSV ingestion.
# ------------------------------------------------------------------

raw_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("claim_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("source_version", StringType(), True),
        StructField("source_updated_at", StringType(), True),
        StructField("incident_date", StringType(), True),
        StructField("reported_date", StringType(), True),
        StructField("arrival_timestamp", StringType(), True),
        StructField("incident_type", StringType(), True),
        StructField("claim_status", StringType(), True),
        StructField("incurred_amount", StringType(), True),
        StructField("paid_amount", StringType(), True),
        StructField("police_reported", StringType(), True),
        StructField("fraud_score", StringType(), True),
        StructField("manual_review_required", StringType(), True),
    ]
)


# ------------------------------------------------------------------
# Read Bronze CSV
# ------------------------------------------------------------------

raw_df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .schema(raw_schema)
    .csv(input_path)
    .withColumn("source_file", input_file_name())
)


# ------------------------------------------------------------------
# Clean and convert Silver data types
# ------------------------------------------------------------------

claims_df = raw_df.select(
    trim(col("event_id")).alias("event_id"),
    trim(col("claim_id")).alias("claim_id"),
    trim(col("policy_id")).alias("policy_id"),
    upper(trim(col("operation"))).alias("operation"),
    trim(col("source_version")).cast("integer").alias("source_version"),
    to_timestamp(
        trim(col("source_updated_at")),
        "yyyy-MM-dd'T'HH:mm:ssXXX",
    ).alias("source_updated_at"),
    to_date(trim(col("incident_date")), "yyyy-MM-dd").alias("incident_date"),
    to_date(trim(col("reported_date")), "yyyy-MM-dd").alias("reported_date"),
    to_timestamp(
        trim(col("arrival_timestamp")),
        "yyyy-MM-dd'T'HH:mm:ssXXX",
    ).alias("arrival_timestamp"),
    trim(col("incident_type")).alias("incident_type"),
    upper(trim(col("claim_status"))).alias("claim_status"),
    trim(col("incurred_amount")).cast("decimal(18,2)").alias("incurred_amount"),
    trim(col("paid_amount")).cast("decimal(18,2)").alias("paid_amount"),
    upper(trim(col("police_reported"))).alias("police_reported"),
    trim(col("fraud_score")).cast("integer").alias("fraud_score"),
    when(
        upper(trim(col("manual_review_required"))).isin("TRUE", "Y", "1"),
        lit(True),
    )
    .when(
        upper(trim(col("manual_review_required"))).isin("FALSE", "N", "0"),
        lit(False),
    )
    .otherwise(lit(None).cast("boolean"))
    .alias("manual_review_required"),
    col("source_file"),
    to_date(lit(process_date), "yyyy-MM-dd").alias("load_date"),
    current_timestamp().alias("silver_processed_at"),
)


# ------------------------------------------------------------------
# Data-quality rules
#
# concat_ws skips null error messages and combines all applicable
# problems into one dq_errors column.
# ------------------------------------------------------------------

claims_with_quality = claims_df.withColumn(
    "dq_errors",
    concat_ws(
        "; ",
        when(
            col("event_id").isNull() | (length(col("event_id")) == 0),
            lit("event_id is missing"),
        ),
        when(
            col("claim_id").isNull() | (length(col("claim_id")) == 0),
            lit("claim_id is missing"),
        ),
        when(
            col("policy_id").isNull() | (length(col("policy_id")) == 0),
            lit("policy_id is missing"),
        ),
        when(
            col("operation").isNull(),
            lit("operation is missing"),
        ),
        when(
            col("operation").isNotNull()
            & (~col("operation").isin("INSERT", "UPDATE", "DELETE")),
            lit("operation is invalid"),
        ),
        when(
            col("source_version").isNull() | (col("source_version") <= 0),
            lit("source_version is invalid"),
        ),
        when(
            col("source_updated_at").isNull(),
            lit("source_updated_at is invalid"),
        ),
        when(
            col("incident_date").isNull(),
            lit("incident_date is invalid"),
        ),
        when(
            col("reported_date").isNull(),
            lit("reported_date is invalid"),
        ),
        when(
            col("reported_date") < col("incident_date"),
            lit("reported_date is before incident_date"),
        ),
        when(
            col("arrival_timestamp").isNull(),
            lit("arrival_timestamp is invalid"),
        ),
        when(
            col("incident_type").isNull()
            | (length(col("incident_type")) == 0),
            lit("incident_type is missing"),
        ),
        when(
            col("claim_status").isNull()
            | (length(col("claim_status")) == 0),
            lit("claim_status is missing"),
        ),
        when(
            col("incurred_amount").isNull() | (col("incurred_amount") < 0),
            lit("incurred_amount is invalid"),
        ),
        when(
            col("paid_amount").isNull() | (col("paid_amount") < 0),
            lit("paid_amount is invalid"),
        ),
        when(
            col("police_reported").isNull()
            | (~col("police_reported").isin("Y", "N", "UNKNOWN")),
            lit("police_reported is invalid"),
        ),
        when(
            col("fraud_score").isNull()
            | (col("fraud_score") < 0)
            | (col("fraud_score") > 100),
            lit("fraud_score is invalid"),
        ),
        when(
            col("manual_review_required").isNull(),
            lit("manual_review_required is invalid"),
        ),
    ),
)


# ------------------------------------------------------------------
# Split valid and invalid records
# ------------------------------------------------------------------

valid_claims = claims_with_quality.filter(
    length(col("dq_errors")) == 0
)

invalid_claims = claims_with_quality.filter(
    length(col("dq_errors")) > 0
)


# ------------------------------------------------------------------
# Deduplicate event IDs
#
# Keep the highest source version, followed by the latest update time.
# ------------------------------------------------------------------

deduplication_window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        desc("source_version"),
        desc("source_updated_at"),
    )
)

valid_claims = (
    valid_claims
    .withColumn(
        "_row_number",
        row_number().over(deduplication_window),
    )
    .filter(col("_row_number") == 1)
    .drop("_row_number", "dq_errors", "load_date")
)


# ------------------------------------------------------------------
# Write Silver and quarantine outputs
#
# load_date is removed from the Parquet data because Glue will infer it
# as a partition column from the load_date=YYYY-MM-DD folder.
# ------------------------------------------------------------------

(
    valid_claims.write
    .mode("overwrite")
    .format("parquet")
    .save(silver_path)
)

(
    invalid_claims
    .drop("load_date")
    .write
    .mode("overwrite")
    .format("parquet")
    .save(quarantine_path)
)

job.commit()