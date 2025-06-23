import os
import time
import logging
import boto3
import polars as pl
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env/.env')   # load environment variables from .env file

# load config from environment variables
INTERVAL = float(os.environ.get("STREAM_INTERVAL", 1.0))  # seconds
BATCH_SIZE = int(os.environ.get("STREAM_BATCH_SIZE", 10))   # number of rows per batch
BUCKET = os.environ.get("S3_BUCKET", "synthetic-financial-data-batch-streaming")    
CSV_PATH = os.environ.get("CSV_PATH", "stream_simulator/dataset/synthetic_financial_data.csv")
CHECKPOINT_FILE = os.environ.get("CHECKPOINT_FILE", "stream_simulator/checkpoint.txt")

# set up logging
logging.basicConfig(level=logging.INFO)     
logger = logging.getLogger("stream_simulator")  # logger name

# load checkpoint to resume from last streamed index
def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        logger.info("No checkpoint file found, starting from the beginning.")
        return  0  
    with open(CHECKPOINT_FILE, "r") as f:   # open file in read mode
        try:
            logger.info("Loading checkpoint from file.")
            return int(f.read())    # read the last streamed index
        except Exception:
            logger.error("Failed to read checkpoint file, starting from the beginning.")
            return 0

# save checkpoint to file for resuming later
def save_checkpoint(idx):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(idx))

def main():
    # initialization
    logger.info("Starting the streaming simulator...")
    logger.info(f"Using S3 bucket: {BUCKET}, batch size: {BATCH_SIZE}, interval: {INTERVAL} seconds")

    # create S3 client and read CSV file
    s3 = boto3.client("s3")
    df = pl.read_csv(CSV_PATH)
    total_rows = len(df)
    idx = load_checkpoint()

    logger.info(f"Resuming streaming at row {idx} out of {total_rows}")

    while idx < total_rows:
        now = datetime.now(timezone.utc)
        partition = f"raw/{now:%Y/%m/%d/%H}/"    # partition path based on current time (year-month-day/hour)
        batch = df[idx:idx+BATCH_SIZE]     # get the next batch of rows
        if batch.is_empty():
            break

        # objects for S3
        file_name = f"{partition}min:{now:%M}_sec:{now:%S}.csv"     # file name with current minute and second
        data = batch.write_csv()

        # the key for the S3 object is the partition path + file name, it's the path
        # the body is the CSV data of the batch, the actual content
        s3.put_object(Bucket=BUCKET, Key=file_name, Body=data)
        logger.info(f"Streamed batch {idx}-{min(idx+BATCH_SIZE-1, total_rows-1)} to {file_name}. Saving checkpoint...")

        idx += BATCH_SIZE
        save_checkpoint(idx)

        time.sleep(INTERVAL)    # to simulate streaming delay

    logger.info("Streaming complete.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Streaming stopped by user.")