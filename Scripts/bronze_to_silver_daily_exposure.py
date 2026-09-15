import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    current_timestamp,
    input_file_name,
    length,
    lit,
    to_date,
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
    "s3://bronze-auto-insurance/daily-exposure/"
    f"daily_exposure_{process_date}.csv"
)

silver_path = (
    "s3://silver-auto-insurance/daily_exposure/"
    f"load_date={process_date}/"
)

quarantine_path = (
    "s3://silver-auto-insurance/quarantine/daily_exposure/"
    f"load_date={process_date}/"
)


# ------------------------------------------------------------------
# Bronze schema
# ------------------------------------------------------------------

raw_schema = StructType(
    [
        StructField("exposure_record_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("age_band", StringType(), True),
        StructField("earned_exposure", StringType(), True),
        StructField("earned_premium", StringType(), True),
        StructField("annual_premium", StringType(), True),
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

exposure_df = raw_df.select(
    trim(col("exposure_record_id")).alias("exposure_record_id"),
    trim(col("policy_id")).alias("policy_id"),
    trim(col("region")).alias("region"),
    trim(col("customer_segment")).alias("customer_segment"),
    trim(col("age_band")).alias("age_band"),
    trim(col("earned_exposure"))
    .cast("decimal(18,8)")
    .alias("earned_exposure"),
    trim(col("earned_premium"))
    .cast("decimal(18,4)")
    .alias("earned_premium"),
    trim(col("annual_premium"))
    .cast("decimal(18,2)")
    .alias("annual_premium"),
    col("source_file"),
    to_date(lit(process_date), "yyyy-MM-dd").alias("load_date"),
    current_timestamp().alias("silver_processed_at"),
)


# ------------------------------------------------------------------
# Identify duplicate IDs and duplicate policy-date records
# ------------------------------------------------------------------

exposure_id_window = Window.partitionBy("exposure_record_id")
policy_window = Window.partitionBy("policy_id")

exposure_df = (
    exposure_df
    .withColumn(
        "_exposure_id_count",
        count(lit(1)).over(exposure_id_window),
    )
    .withColumn(
        "_policy_count",
        count(lit(1)).over(policy_window),
    )
    .withColumn(
        "region_quality_flag",
        when(
            upper(col("region")) == "UNKNOWN_REGION",
            lit("UNKNOWN"),
        ).otherwise(lit("VALID")),
    )
)


# ------------------------------------------------------------------
# Data-quality rules
# ------------------------------------------------------------------

exposure_with_quality = exposure_df.withColumn(
    "dq_errors",
    concat_ws(
        "; ",
        when(
            col("exposure_record_id").isNull()
            | (length(col("exposure_record_id")) == 0),
            lit("exposure_record_id is missing"),
        ),
        when(
            col("policy_id").isNull()
            | (length(col("policy_id")) == 0),
            lit("policy_id is missing"),
        ),
        when(
            col("region").isNull() | (length(col("region")) == 0),
            lit("region is missing"),
        ),
        when(
            col("customer_segment").isNull()
            | (length(col("customer_segment")) == 0),
            lit("customer_segment is missing"),
        ),
        when(
            col("age_band").isNull() | (length(col("age_band")) == 0),
            lit("age_band is missing"),
        ),
        when(
            col("earned_exposure").isNull(),
            lit("earned_exposure is invalid"),
        ),
        when(
            (col("earned_exposure") < 0)
            | (col("earned_exposure") > 1),
            lit("earned_exposure is outside the expected range"),
        ),
        when(
            col("earned_premium").isNull()
            | (col("earned_premium") < 0),
            lit("earned_premium is invalid"),
        ),
        when(
            col("annual_premium").isNull()
            | (col("annual_premium") < 0),
            lit("annual_premium is invalid"),
        ),
        when(
            col("_exposure_id_count") > 1,
            lit("duplicate exposure_record_id"),
        ),
        when(
            col("_policy_count") > 1,
            lit("duplicate policy_id for processing date"),
        ),
    ),
)


# ------------------------------------------------------------------
# Split valid and invalid records
# ------------------------------------------------------------------

valid_exposure = (
    exposure_with_quality
    .filter(length(col("dq_errors")) == 0)
    .drop(
        "_exposure_id_count",
        "_policy_count",
        "dq_errors",
        "load_date",
    )
)

invalid_exposure = (
    exposure_with_quality
    .filter(length(col("dq_errors")) > 0)
    .drop(
        "_exposure_id_count",
        "_policy_count",
        "load_date",
    )
)


# ------------------------------------------------------------------
# Write Silver and quarantine outputs
# ------------------------------------------------------------------

(
    valid_exposure.write
    .mode("overwrite")
    .format("parquet")
    .save(silver_path)
)

(
    invalid_exposure.write
    .mode("overwrite")
    .format("parquet")
    .save(quarantine_path)
)

job.commit()