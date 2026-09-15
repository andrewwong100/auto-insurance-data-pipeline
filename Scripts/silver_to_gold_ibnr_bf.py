"""AWS Glue job: Bornhuetter–Ferguson IBNR by accident month.

STATUS
------
This script has not been run or tested. It is not deployed or connected to
Airflow. Model assumptions require review before use in financial reporting.

METHODOLOGY
-----------
The reported-incurred Bornhuetter–Ferguson (BF) method combines an independently
selected expected loss ratio with an externally selected loss-development
pattern. The pattern describes how much of ultimate loss is expected to be
reflected in reported incurred loss at a given development age.

For each accident-month cohort:

    EP       = earned premium for exposure days in that month, through valuation
    ELR      = independently selected expected ultimate loss / earned premium
    R        = latest reported incurred losses for accidents in that month
    p(age)   = expected fraction of ultimate loss reflected in R at this age
    E        = EP * ELR                       [prior expected ultimate losses]
    IBNR     = E * (1 - p(age))              [estimated unreflected development]
    Ultimate = R + IBNR                     [BF estimated ultimate losses]

IBNR is NOT max(E - R, 0). That would be a different method. When reported losses
are higher than expected, BF can still produce positive IBNR for immature ages.

Here "IBNR" is broad development beyond current reported incurred losses:
it can include both losses from unreported claims and further development of
already reported claims (often called IBNER). It is not a claim-count forecast.

Paid losses must NOT be deducted again from R: reported incurred includes paid
losses plus case outstanding. Future total unpaid loss, if needed separately,
would equal BF estimated ultimate losses minus cumulative paid losses.

ASSUMPTIONS AND LIMITATIONS
--------------------------
1. Currency is CAD, following this synthetic project's convention. Premium and
   claim Silver schemas do not contain currency, so this cannot be verified here.
   Mixed-currency data must be separated or converted before using this model.
2. Reported incurred loss contains indemnity paid plus case reserves, excludes
   an existing IBNR reserve, and excludes the separately generated expenses.
   ELR and the development pattern must use that SAME loss definition and the
   same gross/net-of-reinsurance basis. Otherwise the results double-count or
   mix incompatible amounts. No expense reserve is estimated here.
3. ELR is supplied by the caller; it is not estimated from immature observed
   loss ratios. No assumption values are invented or fitted in this job.
4. One ELR and one development pattern apply to the whole portfolio. This is
   a simplifying assumption: Glass, Collision, and Bodily Injury can develop
   differently. A production model should consider appropriate homogeneous
   groups once sufficient exposure and development history exist.
5. The supplied reported-loss proportions are monotonic, lie in [0, 1], and
   end at 1.0. Ages beyond the final supplied age are assumed fully developed.
   This assumes no further tail development or reopening beyond that horizon.
6. Pattern age 0 means the END of the accident month; age 1 means the END of
   the next month, etc. This is not the same labeling as a triangle whose first
   development column is called "month 1"; convert that pattern before use.
7. For a mid-month valuation, use the last completed month-end age, floored at
   0 for the current accident month. Using age 0 during the current partial
   month is an approximation, explicitly flagged in the output. No interpolation
   or annualization of partial-month premium is performed.
8. Exposure load_date is treated as the exposure business date because the
   current Silver exposure schema has no separate exposure_date. If late-loaded
   exposure represents earlier days, add a business date before using this job.
9. All Silver daily partitions between HISTORY_START_DATE and PROCESS_DATE must
   exist. Presence checks prevent gaps from being treated as zero, but cannot
   prove upstream completeness. Rejected/quarantined Silver rows are not restored.
10. The first exposure date must be the first of a month. Only accident months
    within this history window are valued. Older accident cohorts are excluded,
    so this is not a complete balance-sheet reserve if pre-window claims exist.
11. One latest economic version per claim is selected before summing incurred.
    Conflicting versions, unsupported DELETEs, or invalid inputs fail visibly.
    Claims with no exposure in their accident month fail rather than disappear.
12. These are deterministic point estimates, with no confidence intervals,
    discounting, inflation adjustment, catastrophe model, or parameter uncertainty.
13. Policy-level allocation of IBNR is deliberately NOT performed. Corporate and
    portfolio expenses remain outside this loss model.

REQUIRED GLUE ARGUMENTS / VARIABLES
---------------------------------
--PROCESS_DATE
    Valuation/as-of date, exactly YYYY-MM-DD. The DAG should supply the same
    date used by the Gold reporting job. There is no automatic "today" fallback.
--HISTORY_START_DATE
    First exposure date, exactly YYYY-MM-DD, on the first of a month.
    For the current synthetic project the intended inception is 2026-01-01.
--EXPECTED_LOSS_RATIO
    ELR as a fraction, not a percentage. Must be finite and positive. An ELR
    above 1 is permitted; the model does not assume the portfolio is profitable.
--REPORTED_PROPORTIONS_JSON
    JSON object mapping consecutive age strings "0", "1", ... to proportions.
    Use decimal fractions, not percentages or cumulative development factors.
    If given cumulative age-to-ultimate factors, convert each to 1 / factor
    before passing it. Values are required inputs, not defaults in this script.
--MODEL_VERSION
    Reviewed assumption version, containing only letters, numbers, _ and -.
    Use a new version when changing ELR, pattern, or a prior published result.
JOB_NAME
    Supplied by AWS Glue, rather than manually configured by the caller.

INPUTS
------
s3://silver-auto-insurance/daily_exposure/load_date=YYYY-MM-DD/
s3://silver-auto-insurance/claims_events/load_date=YYYY-MM-DD/
Payments and expenses are not inputs to this reported-incurred BF calculation.

OUTPUT GRAIN AND USE
--------------------
One row per accident_month, within one valuation date and model version:
s3://gold-auto-insurance/ibnr_bf_monthly/
    as_of_date=YYYY-MM-DD/model_version=VERSION/

To obtain portfolio IBNR, SUM estimated_ibnr over accident months for exactly
ONE as_of_date and ONE model_version. Never sum overlapping valuation snapshots.
To adjust the existing Gold result, subtract this portfolio IBNR from the
reported result before IBNR for the SAME history window and valuation date.

This script writes Parquet only. It does not create Catalog tables, register
partitions, alter Gold reports, or modify the DAG. Future Catalog partition
keys should be as_of_date STRING and model_version STRING. These two keys are
encoded in directories rather than duplicated inside Parquet columns.

The output version must not already exist. errorifexists protects old results,
but a failed write may leave an incomplete directory. Consumers must wait for
Glue success. Review/remove an incomplete version or select a new version for
a retry; never assume directory existence proves a successful publication.
Run only one writer for a given valuation-date/model-version combination.
"""

import csv
import json
import re
import sys
from datetime import date, timedelta
from decimal import Decimal

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F, types as T


def fail_if_rows(frame, message):
    """Stop rather than publish an estimate based on ambiguous economic data."""
    examples = frame.limit(5).collect()
    if examples:
        raise ValueError(f"{message}: {[row.asDict() for row in examples]}")


def parse_iso_date(value, argument_name):
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError(f"{argument_name} must be YYYY-MM-DD")
    return date.fromisoformat(value)


def unique_json_object(pairs):
    """Prevent a repeated JSON age from silently replacing an assumption."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate development age: {key}")
        result[key] = value
    return result


def parse_assumptions(elr_text, pattern_text):
    # Decimal arithmetic avoids conversion through binary floating-point values.
    # The supported assumption type is decimal(18,8): up to eight decimal places.
    elr = Decimal(elr_text)
    if (
        not elr.is_finite()
        or elr <= 0
        or elr >= Decimal("10000000000")
        or elr != elr.quantize(Decimal("0.00000001"))
    ):
        raise ValueError("ELR must be positive and exactly representable as decimal(18,8)")

    supplied = json.loads(
        pattern_text,
        parse_float=Decimal,
        parse_int=Decimal,
        object_pairs_hook=unique_json_object,
    )
    if not isinstance(supplied, dict) or not supplied:
        raise ValueError("REPORTED_PROPORTIONS_JSON must be a nonempty JSON object")

    pattern = {}
    for key, raw_value in supplied.items():
        if not re.fullmatch(r"0|[1-9][0-9]*", key):
            raise ValueError("Pattern keys must be nonnegative integer age strings")
        # Boolean values are not numeric reporting assumptions.
        if isinstance(raw_value, bool) or not isinstance(raw_value, (Decimal, str)):
            raise ValueError(f"Invalid reporting proportion for age {key}")
        value = Decimal(raw_value)
        if (
            not value.is_finite()
            or not Decimal("0") <= value <= Decimal("1")
            or value != value.quantize(Decimal("0.00000001"))
        ):
            raise ValueError("Reporting proportions must be in [0,1], with at most 8 decimal places")
        pattern[int(key)] = value

    ages = sorted(pattern)
    if ages != list(range(len(ages))):
        raise ValueError("Supply consecutive development ages starting at 0")
    if any(pattern[b] < pattern[a] for a, b in zip(ages, ages[1:])):
        raise ValueError("Reporting proportions must be nondecreasing")
    if pattern[ages[-1]] != Decimal("1"):
        raise ValueError("Final reporting proportion must be 1.0; select tail development explicitly")
    return elr, pattern


def read_silver_history(spark, s3, dataset, start, end):
    required = {
        (start + timedelta(days=i)).isoformat()
        for i in range((end - start).days + 1)
    }
    available = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket="silver-auto-insurance", Prefix=f"{dataset}/"
    ):
        for item in page.get("Contents", []):
            parts = item["Key"].split("/")
            if (
                len(parts) >= 3
                and parts[1].startswith("load_date=")
                and parts[-1].endswith(".parquet")
            ):
                available.add(parts[1].split("=", 1)[1])
    missing = sorted(required - available)
    if missing:
        raise ValueError(f"{dataset}: missing Silver Parquet dates {missing}")

    root = f"s3://silver-auto-insurance/{dataset}/"
    return spark.read.option("basePath", root).parquet(
        *[f"{root}load_date={day}/" for day in sorted(required)]
    )


def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "PROCESS_DATE", "HISTORY_START_DATE",
        "EXPECTED_LOSS_RATIO", "REPORTED_PROPORTIONS_JSON", "MODEL_VERSION",
    ])
    start = parse_iso_date(args["HISTORY_START_DATE"], "HISTORY_START_DATE")
    end = parse_iso_date(args["PROCESS_DATE"], "PROCESS_DATE")
    if start > end or start.day != 1:
        raise ValueError("History must begin on a month's first day, no later than PROCESS_DATE")
    elr, pattern = parse_assumptions(
        args["EXPECTED_LOSS_RATIO"], args["REPORTED_PROPORTIONS_JSON"]
    )
    version = args["MODEL_VERSION"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", version):
        raise ValueError("MODEL_VERSION must be 1–100 letters, digits, underscores or hyphens")

    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Fail on numeric overflow instead of silently producing null loss estimates.
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    job = Job(context)
    job.init(args["JOB_NAME"], args)

    s3 = boto3.client("s3")
    exposure = read_silver_history(spark, s3, "daily_exposure", start, end)
    claims = read_silver_history(spark, s3, "claims_events", start, end)

    # EP: use one accepted premium value per policy/day. Repeated identical
    # values collapse; conflicting values are a source-quality failure.
    exposure = exposure.select("policy_id", "load_date", "earned_premium").dropDuplicates()
    fail_if_rows(exposure.filter(
        F.col("policy_id").isNull() | (F.length(F.trim("policy_id")) == 0)
        | F.col("earned_premium").isNull() | (F.col("earned_premium") < 0)
    ), "Invalid exposure inputs")
    fail_if_rows(exposure.groupBy("policy_id", "load_date").count().filter("count > 1"),
                 "Conflicting policy-day premiums")

    # R: source events are balance snapshots, not additive loss transactions.
    # Resolve duplicates, then choose the highest source_version for each claim.
    claim_fields = [
        "claim_id", "policy_id", "source_version", "source_updated_at",
        "incident_date", "reported_date", "operation", "incurred_amount",
    ]
    claims = claims.select(*claim_fields).dropDuplicates()
    fail_if_rows(claims.filter(
        F.col("claim_id").isNull() | (F.length(F.trim("claim_id")) == 0)
        | F.col("policy_id").isNull() | (F.length(F.trim("policy_id")) == 0)
        | F.col("source_version").isNull() | (F.col("source_version") <= 0)
        | F.col("incident_date").isNull() | F.col("reported_date").isNull()
        | (F.col("reported_date") < F.col("incident_date"))
        | (F.col("reported_date") > F.lit(end))
        | F.col("source_updated_at").isNull()
        | (F.to_date("source_updated_at") > F.lit(end))
        | F.col("incurred_amount").isNull() | (F.col("incurred_amount") < 0)
    ), "Invalid or future-dated claim economic records")
    fail_if_rows(claims.groupBy("claim_id", "source_version").count().filter("count > 1"),
                 "Conflicting economic fields for the same claim version")
    fail_if_rows(claims.groupBy("claim_id").agg(
        F.countDistinct("policy_id").alias("policy_count")
    ).filter("policy_count > 1"), "Claim changes policy across versions")

    latest = (claims.withColumn("_rank", F.row_number().over(
        Window.partitionBy("claim_id").orderBy(
            F.desc("source_version"), F.desc("source_updated_at")
        )
    )).filter("_rank = 1").drop("_rank"))
    fail_if_rows(latest.filter(
        F.col("operation").isNull() | ~F.col("operation").isin("INSERT", "UPDATE")
    ), "Unsupported claim operation: resolve deletes/reversals before reserving")

    premiums = (exposure.withColumn(
        "accident_month", F.trunc(F.to_date("load_date"), "month")
    ).groupBy("accident_month").agg(
        F.sum("earned_premium").cast("decimal(28,4)").alias("earned_premium")
    ))
    losses = (latest.filter(F.col("incident_date").between(start, end))
        .withColumn("accident_month", F.trunc("incident_date", "month"))
        .groupBy("accident_month").agg(
            F.count("claim_id").alias("reported_claim_count"),
            F.sum("incurred_amount").cast("decimal(28,4)").alias("reported_incurred_amount"),
        ))

    # Full join preserves loss-only cohorts so they can be rejected explicitly.
    # Exposure-only cohorts remain: zero reported loss does not imply zero IBNR.
    cohorts = premiums.join(losses, "accident_month", "full").fillna({
        "earned_premium": 0, "reported_incurred_amount": 0, "reported_claim_count": 0,
    })
    fail_if_rows(cohorts.filter(
        (F.col("earned_premium") <= 0) & (F.col("reported_incurred_amount") > 0)
    ), "Loss cohort has no positive premium denominator")

    # age: completed month-end development periods under the convention above.
    is_month_end = (end + timedelta(days=1)).day == 1
    age_adjustment = 0 if is_month_end else 1
    cohorts = cohorts.withColumn("development_age_months", F.greatest(
        F.lit(0),
        (F.year(F.lit(end)) - F.year("accident_month")) * 12
        + F.month(F.lit(end)) - F.month("accident_month") - F.lit(age_adjustment),
    ))
    last_age = max(pattern)
    pattern_schema = T.StructType([
        T.StructField("pattern_age", T.IntegerType(), False),
        T.StructField("reported_proportion", T.DecimalType(18, 8), False),
    ])
    pattern_frame = spark.createDataFrame(
        [(age, pattern[age]) for age in sorted(pattern)], schema=pattern_schema
    )
    cohorts = (cohorts.withColumn("_pattern_age", F.least(
        F.col("development_age_months"), F.lit(last_age)
    )).join(F.broadcast(pattern_frame),
            F.col("_pattern_age") == F.col("pattern_age"), "left")
        .drop("_pattern_age", "pattern_age"))

    # Monetary outputs retain four decimal places, matching Gold reporting.
    # Actual ELR, age and pattern are saved with each result for auditability.
    result = (cohorts
        .withColumn("expected_loss_ratio", F.lit(str(elr)).cast("decimal(18,8)"))
        .withColumn("expected_ultimate_losses", (
            F.col("earned_premium") * F.col("expected_loss_ratio")
        ).cast("decimal(28,4)"))
        .withColumn("estimated_ibnr", (
            F.col("expected_ultimate_losses") * (F.lit(1) - F.col("reported_proportion"))
        ).cast("decimal(28,4)"))
        .withColumn("estimated_ultimate_losses", (
            F.col("reported_incurred_amount") + F.col("estimated_ibnr")
        ).cast("decimal(28,4)"))
        .withColumn("valuation_date", F.lit(end).cast("date"))
        .withColumn("history_start_date", F.lit(start).cast("date"))
        .withColumn("currency", F.lit("CAD"))
        .withColumn("model_method", F.lit("BF_REPORTED_INCURRED"))
        .withColumn("is_month_end_valuation", F.lit(is_month_end))
        .withColumn("is_partial_accident_month",
                    F.last_day("accident_month") > F.lit(end))
        .withColumn("tail_assumed_fully_developed",
                    F.col("development_age_months") > F.lit(last_age))
        .withColumn("reporting_pattern_json", F.lit(args["REPORTED_PROPORTIONS_JSON"]))
        .withColumn("processed_at", F.current_timestamp())
    )

    output = (
        "s3://gold-auto-insurance/ibnr_bf_monthly/"
        f"as_of_date={end.isoformat()}/model_version={version}/"
    )
    # Only the supplied version is written. Existing results are never replaced.
    result.coalesce(1).write.mode("errorifexists").parquet(output)
    print(json.dumps({
        "output": output,
        "model_version": version,
        "method": "BF_REPORTED_INCURRED",
        "expected_loss_ratio": str(elr),
        "valuation_date": end.isoformat(),
        "history_start_date": start.isoformat(),
    }))
    job.commit()


if __name__ == "__main__":
    main()
