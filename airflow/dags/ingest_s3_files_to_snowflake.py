from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3
import logging
import pendulum

# airflow variables for S3 configuration
s3_bucket = Variable.get("S3_BUCKET", default="ssynthetic-financial-data-batch-streaming")
s3_prefix_raw = Variable.get("S3_PREFIX", default="rraw/")
s3_prefix_processed = "processed/"
s3_conn_id = "aws"

# airflow variables for Snowflake configuration
snowflake_conn_id = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_default")
snowflake_database = Variable.get("SNOWFLAKE_DATABASE", default_var="MY_DATABASE")
snowflake_schema = Variable.get("SNOWFLAKE_SCHEMA", default_var="PUBLIC")
snowflake_stage = Variable.get("SNOWFLAKE_STAGE", default_var="MY_STAGE")
snowflake_table = Variable.get("SNOWFLAKE_TABLE", default_var="MY_TABLE")
snowflake_file_format = Variable.get("SNOWFLAKE_FILE_FORMAT", default_var="CSV_FORMAT")

scheduled_interval = 5  # in minutes

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("airflow_ingest_dag")


# Define default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="ingest_s3_to_snowflake_taskflow",
    start_date=pendulum.now("UTC").subtract(minutes=1),
    schedule=f"*/{scheduled_interval} * * * *",
    catchup=False,
    default_args=default_args,
    tags=["batch", "streaming", "s3", "snowflake"],
)
def ingest_s3_new_files():

    @task()
    def get_last_run_time(prev_start_date_success=None):
        # i can try this prev_start_date_success 
        return prev_start_date_success    # to return the last run time

    @task()
    def list_new_files(last_run_time: datetime) -> list:
        hook = S3Hook(aws_conn_id=s3_conn_id)
        s3 = hook.get_conn()

        logger = logging.getLogger("airflow.task")
        logger.info(f"Looking for files uploaded after: {last_run_time}")

        # Build prefix just to limit scope. You can broaden if needed.
        prefix = s3_prefix_raw  # 'raw/'

        paginator = s3.get_paginator("list_objects_v2")     # paginator for large buckets (> 1000 objects)
        page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)     
        
        new_files = []

        for page in page_iterator:
            for obj in page.get("Contents", []):
                if obj["LastModified"] > last_run_time:
                    # logger.info(f"New file found: {obj['Key']}, LastModified: {obj['LastModified']}")
                    new_files.append(obj["Key"])    # the key has the full path including prefix

        logger.info(f"Found {len(new_files)} new files since last run.")
        return new_files

    @task()
    def process_files(file_keys: list):
        logger = logging.getLogger("airflow.task")
        for key in file_keys:
            logger.info(f"Processing file: {key}")
            # TODO: Ingest logic here

        return file_keys

    # DAG flow
    last_run = get_last_run_time()
    new_files = list_new_files(last_run)
    process_files(new_files)


dag_instance = ingest_s3_new_files()
