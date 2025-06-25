USE DATABASE STREAMING_DB;

-- A stage points to a location in an external storage service, such as Amazon S3
-- and allows Snowflake to read data from or write data to that location.
CREATE OR REPLACE STAGE RAW_STAGE
  URL = 's3://synthetic-financial-data-batch-streaming/raw/'
  STORAGE_INTEGRATION = my_s3_int
  FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT');


LIST @RAW_STAGE;