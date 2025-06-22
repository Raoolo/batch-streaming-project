from airflow.decorators import dag, task
from airflow.sdk import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3
import logging

# Define default args
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id="ingest_s3_to_snowflake_taskflow",
    start_date=datetime(2024, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["batch", "streaming", "s3", "snowflake"],
)
def ingest_s3_to_snowflake():
    @task()
    def ingest_latest_batch():
        # Get values from Airflow Variables, or use a default if not set
        s3_bucket = Variable.get("S3_BUCKET", default_var="synthetic-financial-data-batch-streaming")
        s3_prefix = Variable.get("S3_PREFIX", default_var="raw/")
        
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("airflow_ingest")

        hook = S3Hook(aws_conn_id='aws')  
        #s3 = hook.get_client_type('s3')
        #response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        keys = hook.list_keys(bucket_name=s3_bucket, prefix=s3_prefix)
        logger.info(f"Found {len(keys)} files in S3 bucket {s3_bucket} with prefix {s3_prefix}")
        #files = [obj['Key'] for obj in response.get('Contents', [])]
        #logger.info(f"Found {len(files)} files: {files}")
        # TODO: Download/process/upload to Snowflake

    ingest_latest_batch()

dag_instance = ingest_s3_to_snowflake()
