-- 1. Switch to the right role
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION my_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::331317595486:role/snowflake-s3-integration-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://synthetic-financial-data-batch-streaming/');

-- Get the generated thing
DESC INTEGRATION my_s3_int;