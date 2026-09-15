"""Standalone AWS Glue Spark job. One load-date partition per run.

Transaction IDs are immutable: identical rows collapse; conflicting rows for
the same ID are quarantined. No cross-partition deduplication or foreign-key
lookup is performed. Run reconciliation separately before Gold aggregation.
"""
import csv
import io
import json
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

DATASET = 'expense_transactions'
FIELDS = ['expense_id', 'expense_date', 'expense_category', 'expense_scope', 'amount', 'currency', 'claim_id', 'policy_id', 'region', 'source_system', 'created_at', 'updated_at']
ID_FIELD = 'expense_id'
DATE_FIELD = 'expense_date'
PREFIX = DATASET.replace("_", "-")


def normalize(values, process_date):
    """Pure validation shared by the Spark UDF and offline tests."""
    row = {key: (str(values.get(key)).strip() or None)
           if values.get(key) is not None else None for key in FIELDS}
    errors = []
    required = [ID_FIELD, DATE_FIELD, "amount", "currency", "source_system",
                "created_at", "updated_at"]
    if DATASET == "claim_payments":
        required += ["claim_id", "policy_id", "payment_type", "payee_type",
                     "source_event_id", "source_version"]
    else:
        required += ["expense_category", "expense_scope"]
    for key in required:
        if row[key] is None:
            errors.append(key + " is missing")
    for key in ["currency", "payment_type", "payee_type", "expense_category", "expense_scope"]:
        if key in row and row[key]:
            row[key] = row[key].upper()
    if row["currency"] and not re.fullmatch(r"[A-Z]{3}", row["currency"]):
        errors.append("currency must be a three-letter code")
    for key, pattern in [("claim_id", r"CLM-[0-9]+"), ("policy_id", r"POL-[0-9]+")]:
        if row.get(key) and not re.fullmatch(pattern, row[key]):
            errors.append(key + " has invalid format")
    try:
        value = Decimal(row["amount"] or "invalid")
        if not value.is_finite() or value <= 0 or value >= Decimal("10000000000000000"):
            raise ValueError()
        if value != value.quantize(Decimal("0.01")):
            raise ValueError()
        row["amount"] = format(value, ".2f")
    except (InvalidOperation, ValueError):
        errors.append("amount must be positive decimal(18,2), without rounding")
        row["amount"] = None
    transaction_date = None
    try:
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", row[DATE_FIELD] or ""):
            raise ValueError()
        transaction_date = date.fromisoformat(row[DATE_FIELD])
        if transaction_date > date.fromisoformat(process_date):
            errors.append(DATE_FIELD + " is after process date")
    except ValueError:
        errors.append(DATE_FIELD + " is invalid")
        row[DATE_FIELD] = None
    times = {}
    for key in ["created_at", "updated_at"]:
        try:
            stamp = datetime.fromisoformat((row[key] or "").replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                raise ValueError()
            stamp = stamp.astimezone(timezone.utc)
            times[key] = stamp
            row[key] = stamp.isoformat()
            if stamp.date() > date.fromisoformat(process_date):
                errors.append(key + " is after process date")
        except ValueError:
            errors.append(key + " must be an ISO timestamp with timezone")
            row[key] = None
    if len(times) == 2 and times["updated_at"] < times["created_at"]:
        errors.append("updated_at is before created_at")
    if transaction_date and "created_at" in times and times["created_at"].date() < transaction_date:
        errors.append("created_at is before transaction date")
    if DATASET == "claim_payments":
        if row["payment_type"] not in ["INDEMNITY"]:
            errors.append("payment_type is unsupported")
        if row["payee_type"] not in ["CLAIMANT", "REPAIR_SHOP", "RENTAL_PROVIDER",
                                     "MEDICAL_PROVIDER", "GLASS_REPAIR_VENDOR"]:
            errors.append("payee_type is invalid")
        try:
            if not re.fullmatch(r"[0-9]+", row["source_version"] or ""):
                raise ValueError()
            version = int(row["source_version"])
            if not 1 <= version <= 2147483647:
                raise ValueError()
            row["source_version"] = str(version)
        except ValueError:
            errors.append("source_version must be a positive integer")
            row["source_version"] = None
    else:
        categories = {
            "CLAIM_ADJUSTING": "CLAIM", "CLAIM_SETTLEMENT_ADMIN": "CLAIM",
            "POLICY_INSPECTION": "POLICY", "BROKER_COMMISSION": "POLICY",
            "REGIONAL_MARKETING": "PORTFOLIO", "CLOUD_INFRASTRUCTURE": "CORPORATE",
            "FINANCE_HR_PAYROLL": "CORPORATE", "OFFICE_RENT": "CORPORATE",
        }
        scope = row["expense_scope"]
        if scope not in ["CLAIM", "POLICY", "PORTFOLIO", "CORPORATE"]:
            errors.append("expense_scope is invalid")
        if row["expense_category"] not in categories:
            errors.append("expense_category is unsupported")
        elif categories[row["expense_category"]] != scope:
            errors.append("expense_category does not match expense_scope")
        if scope == "CLAIM" and (not row["claim_id"] or not row["policy_id"]):
            errors.append("CLAIM expense requires claim_id and policy_id")
        if scope == "POLICY" and (not row["policy_id"] or row["claim_id"]):
            errors.append("POLICY expense requires policy_id and null claim_id")
        if scope in ["PORTFOLIO", "CORPORATE"] and (row["claim_id"] or row["policy_id"]):
            errors.append("overhead must not reference a claim or policy")
        if scope == "CORPORATE" and row["region"]:
            errors.append("CORPORATE expense must have null region")
    row["dq_errors"] = "; ".join(errors)
    return row


def parse_line(line):
    # Generated raw files are single-line CSV records. Reject malformed quoting
    # and extra/missing fields explicitly; Spark CSV silently drops extra fields.
    try:
        values = next(csv.reader(io.StringIO(line), strict=True))
        if len(values) != len(FIELDS):
            raise ValueError("wrong CSV field count")
        return dict(zip(FIELDS, values)), None
    except (csv.Error, ValueError, StopIteration) as error:
        return dict.fromkeys(FIELDS), "malformed CSV: " + str(error)


def main():
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import Window, functions as F, types as T

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "PROCESS_DATE"])
    process_date = args["PROCESS_DATE"]
    if date.fromisoformat(process_date).isoformat() != process_date:
        raise ValueError("PROCESS_DATE must be YYYY-MM-DD")
    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    input_path = f"s3://bronze-auto-insurance/{PREFIX}/{DATASET}_{process_date}.csv"
    silver_path = f"s3://silver-auto-insurance/{DATASET}/load_date={process_date}/"
    quarantine_path = f"s3://silver-auto-insurance/quarantine/{DATASET}/load_date={process_date}/"

    # Exactly one source object per day. Validate its header before writing.
    import boto3
    key = f"{PREFIX}/{DATASET}_{process_date}.csv"
    response = boto3.client("s3").get_object(Bucket="bronze-auto-insurance", Key=key)
    body = response["Body"]
    try:
        first_line = next(body.iter_lines()).decode("utf-8-sig")
    finally:
        body.close()
    if next(csv.reader([first_line])) != FIELDS:
        raise ValueError("Bronze CSV header/order does not match expected schema")

    schema = T.StructType([T.StructField(k, T.StringType(), True)
                           for k in FIELDS + ["dq_errors"]])
    def transform(line):
        values, error = parse_line(line)
        result = normalize(values, process_date)
        if error:
            result["dq_errors"] = error + "; " + result["dq_errors"]
        return result
    normalize_udf = F.udf(transform, schema)
    raw = spark.read.text(input_path).withColumn("source_file", F.input_file_name())
    raw = raw.withColumn("value", F.regexp_replace("value", "^\\ufeff", ""))
    raw = raw.filter(F.col("value") != F.lit(first_line))
    checked = raw.select(F.col("value").alias("raw_record"), "source_file",
                         normalize_udf("value").alias("clean")).select("raw_record", "source_file", "clean.*")
    checked = checked.withColumn("silver_processed_at", F.current_timestamp()).cache()
    input_count = checked.count()
    invalid = checked.filter(F.length("dq_errors") > 0)
    candidates = checked.filter(F.length("dq_errors") == 0)
    candidate_count = candidates.count()
    # Deterministic representative for equivalent normalized rows.
    rank = Window.partitionBy(*FIELDS).orderBy("raw_record", "source_file")
    unique = candidates.withColumn("_rank", F.row_number().over(rank)).filter("_rank = 1").drop("_rank")
    unique = unique.withColumn("_id_count", F.count(F.lit(1)).over(Window.partitionBy(ID_FIELD))).cache()
    conflicts = unique.filter("_id_count > 1").drop("_id_count").withColumn("dq_errors", F.lit("conflicting transaction ID within load date"))
    valid = unique.filter("_id_count = 1").drop("_id_count", "dq_errors", "raw_record")
    quarantine = invalid.unionByName(conflicts)
    valid = valid.withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    valid = valid.withColumn(DATE_FIELD, F.to_date(DATE_FIELD, "yyyy-MM-dd"))
    for key in ["created_at", "updated_at"]:
        valid = valid.withColumn(key, F.to_timestamp(key))
    if DATASET == "claim_payments":
        valid = valid.withColumn("source_version", F.col("source_version").cast("int"))
    metrics = {"dataset": DATASET, "process_date": process_date,
               "input_rows": input_count, "valid_rows": valid.count(),
               "quarantine_rows": quarantine.count(),
               "duplicate_rows_collapsed": candidate_count - unique.count()}
    assert metrics["input_rows"] == sum(metrics[k] for k in ["valid_rows", "quarantine_rows", "duplicate_rows_collapsed"])
    # Overwrite only the requested partition, including empty results on reruns.
    # Two S3 writes are not atomic; downstream tasks must wait for job success.
    valid.write.mode("overwrite").parquet(silver_path)
    quarantine.write.mode("overwrite").parquet(quarantine_path)
    print(json.dumps(metrics, sort_keys=True))
    unique.unpersist()
    checked.unpersist()
    job.commit()


if __name__ == "__main__":
    main()
