from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3
import logging
import pendulum

# get values from airflow variables
s3_bucket = Variable.get("S3_BUCKET", default="ssynthetic-financial-data-batch-streaming")
s3_prefix_raw = Variable.get("S3_PREFIX", default="rraw/")
s3_prefix_processed = "processed/"
s3_conn_id = "aws"
scheduled_interval = 5  # every 2 minutes

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
    def get_last_run_time(data_interval_start=None):
        return data_interval_start.subtract(minutes=scheduled_interval)      # to return the last run time

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
