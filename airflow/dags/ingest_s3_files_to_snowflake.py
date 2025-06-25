from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import boto3
import logging
import pendulum

# airflow variables for S3 configuration
s3_bucket = Variable.get("S3_BUCKET", default="synthetic-financial-data-batch-streaming")
s3_prefix_raw = Variable.get("S3_PREFIX", default="raw/")
s3_prefix_processed = "processed/"
s3_conn_id = "aws"

# airflow variables for Snowflake configuration TODO: still to set these in Airflow UI
snowflake_conn_id = Variable.get("SNOWFLAKE_CONN_ID", default="snowflake_connection")
snowflake_database = Variable.get("SNOWFLAKE_DATABASE", default="STREAMING_DB")
snowflake_raw_schema = Variable.get("SNOWFLAKE_SCHEMA", default="RAW")
snowflake_processed_schema = Variable.get("SNOWFLAKE_PROCESSED_SCHEMA", default="PROCESSED")
snowflake_stage = Variable.get("SNOWFLAKE_STAGE", default="RAW_STAGE")
snowflake_table = Variable.get("SNOWFLAKE_TABLE", default="TRANSACTIONS_RAW")
snowflake_file_format = Variable.get("SNOWFLAKE_FILE_FORMAT", default="CSV_FORMAT")

scheduled_interval = 5  # in minutes

logging.basicConfig(level=logging.INFO)

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
        snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        # sf_conn = snowflake_hook.get_conn()

        for key in file_keys:
            try:
                logger.info(f"Processing file: {key}")
                file_path = f'@{snowflake_stage}/{key.split(s3_prefix_raw)[-1]}'
                copy_query = f"""
                    COPY INTO {snowflake_raw_schema}.{snowflake_table}
                    FROM {file_path}
                    FILE_FORMAT = (FORMAT_NAME = '{snowflake_file_format}')
                    ON_ERROR = 'CONTINUE';
                """
                snowflake_hook.run(copy_query)
                logger.info(f"Executed COPY INTO for file: {key}")
            except Exception as e:
                logger.error(f"Error processing file {key}: {e}")
                continue

            
            # Move processed file
            # s3_hook = S3Hook(aws_conn_id=s3_conn_id)
            # source_key = key
            # destination_key = key.replace(s3_prefix_raw, s3_prefix_processed)
            # s3_hook.copy_object(
            #     source_bucket_name=s3_bucket,
            #     dest_bucket_name=s3_bucket,
            #     source_bucket_key=source_key,
            #     dest_bucket_key=destination_key,
            # )
            # s3_hook.delete_objects(bucket=s3_bucket, keys=[source_key])


    # DAG flow
    last_run = get_last_run_time()
    new_files = list_new_files(last_run)
    process_files(new_files)


# dag_instance = ingest_s3_new_files()
