-- Create a file format for CSV files 

USE DATABASE STREAMING_DB;      -- wasnt working without this line

CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 0
  NULL_IF = ('NULL','');