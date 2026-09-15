from copy import deepcopy
from datetime import date, timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


AWS_CONN_ID = "aws_default"
AWS_REGION = "ca-central-1"
GLUE_IAM_ROLE = "auto-insurance-glue-role"
SILVER_DATABASE = "auto_insurance_silver"

PROCESS_DATE = "{{ dag_run.conf.get('process_date', params.process_date) }}"


def validate_process_date(process_date):
    """Reject invalid dates before starting any of the four branches."""
    if not isinstance(process_date, str) or date.fromisoformat(process_date).isoformat() != process_date:
        raise ValueError("process_date must be a calendar date in YYYY-MM-DD format")
    return process_date


def register_silver_partition(table_name, process_date):
    """Register just this successful run's partition; reruns are safe."""
    validate_process_date(process_date)
    if table_name not in {"claim_payments", "expense_transactions"}:
        raise ValueError("Unexpected Silver table")
    client = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID, client_type="glue", region_name=AWS_REGION,
    ).get_conn()
    table = client.get_table(DatabaseName=SILVER_DATABASE, Name=table_name)["Table"]
    keys = [(key["Name"], key["Type"]) for key in table.get("PartitionKeys", [])]
    if keys != [("load_date", "string")]:
        raise ValueError(f"Unexpected partition keys for {table_name}: {keys}")
    descriptor = deepcopy(table["StorageDescriptor"])
    root = f"s3://silver-auto-insurance/{table_name}"
    if descriptor["Location"].rstrip("/") != root:
        raise ValueError(f"Unexpected Silver location for {table_name}")
    location = f"{root}/load_date={process_date}/"
    descriptor["Location"] = location
    try:
        client.create_partition(
            DatabaseName=SILVER_DATABASE, TableName=table_name,
            PartitionInput={"Values": [process_date], "StorageDescriptor": descriptor},
        )
    except client.exceptions.AlreadyExistsException:
        existing = client.get_partition(
            DatabaseName=SILVER_DATABASE, TableName=table_name,
            PartitionValues=[process_date],
        )["Partition"]
        if existing["StorageDescriptor"]["Location"].rstrip("/") != location.rstrip("/"):
            raise ValueError(f"Existing partition for {table_name} points to a different location")
    return location


default_args = {
    "owner": "auto-insurance",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="auto_insurance_bronze_to_silver",
    description="Process four Bronze datasets through Silver into cumulative Gold reports",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "process_date": "2026-01-06",
    },
    tags=[
        "auto-insurance",
        "bronze",
        "silver",
        "gold",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    validate_date = PythonOperator(
        task_id="validate_process_date",
        python_callable=validate_process_date,
        op_kwargs={"process_date": PROCESS_DATE},
        retries=0,
    )

    wait_for_claims = S3KeySensor(
        task_id="wait_for_claims_file",
        bucket_name="bronze-auto-insurance",
        bucket_key=f"claims-events/claim_events_{PROCESS_DATE}.csv",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    wait_for_exposure = S3KeySensor(
        task_id="wait_for_exposure_file",
        bucket_name="bronze-auto-insurance",
        bucket_key=f"daily-exposure/daily_exposure_{PROCESS_DATE}.csv",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    transform_claims = GlueJobOperator(
        task_id="transform_claims_to_silver",
        job_name="auto-insurance-bronze-to-silver-claims",
        script_location="s3://scripts-auto-insurance/glue/bronze_to_silver_claim_events.py",
        iam_role_name=GLUE_IAM_ROLE,
        script_args={
            "--PROCESS_DATE": PROCESS_DATE,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        stop_job_run_on_kill=True,
        verbose=True,
    )

    transform_exposure = GlueJobOperator(
        task_id="transform_exposure_to_silver",
        job_name="auto-insurance-bronze-to-silver-exposure",
        script_location="s3://scripts-auto-insurance/glue/bronze_to_silver_daily_exposure.py",
        iam_role_name=GLUE_IAM_ROLE,
        script_args={
            "--PROCESS_DATE": PROCESS_DATE,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        wait_for_completion=True,
        stop_job_run_on_kill=True,
        verbose=True,
    )

    # Use the existing, verified Glue jobs. No duplicate auto-* jobs or
    # alternative S3 script copies are needed. Preserve their saved settings.
    new_partition_tasks = []
    for dataset, label, job_name in [
        ("claim_payments", "payments", "bronze_to_silver_claim_payments"),
        ("expense_transactions", "expenses", "bronze_to_silver_expense_transactions"),
    ]:
        wait_for_file = S3KeySensor(
            task_id=f"wait_for_{label}_file",
            bucket_name="bronze-auto-insurance",
            bucket_key=f"{dataset.replace('_', '-')}/{dataset}_{PROCESS_DATE}.csv",
            aws_conn_id=AWS_CONN_ID,
            region_name=AWS_REGION,
            poke_interval=30,
            timeout=600,
            mode="reschedule",
        )
        transform = GlueJobOperator(
            task_id=f"transform_{label}_to_silver",
            job_name=job_name,
            script_args={"--PROCESS_DATE": PROCESS_DATE},
            aws_conn_id=AWS_CONN_ID,
            region_name=AWS_REGION,
            update_config=False,
            replace_script_file=False,
            wait_for_completion=True,
            stop_job_run_on_kill=True,
            sleep_before_return=10,
            verbose=True,
        )
        register = PythonOperator(
            task_id=f"register_{label}_partition",
            python_callable=register_silver_partition,
            op_kwargs={"table_name": dataset, "process_date": PROCESS_DATE},
        )
        validate_date >> wait_for_file >> transform >> register
        new_partition_tasks.append(register)

    transform_gold = GlueJobOperator(
        task_id="transform_silver_to_gold",
        job_name="auto-insurance-silver-to-gold",
        script_args={
            "--PROCESS_DATE": PROCESS_DATE,
            "--HISTORY_START_DATE": "2026-01-01",
            "--TEST_MODE": "false",
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_REGION,
        update_config=False,
        replace_script_file=False,
        wait_for_completion=True,
        stop_job_run_on_kill=True,
        sleep_before_return=10,
        verbose=True,
    )

    complete = EmptyOperator(
        task_id="complete",
    )

    start >> validate_date
    validate_date >> [
        wait_for_claims,
        wait_for_exposure,
    ]

    wait_for_claims >> transform_claims
    wait_for_exposure >> transform_exposure

    [
        transform_claims,
        transform_exposure,
        *new_partition_tasks,
    ] >> transform_gold >> complete
