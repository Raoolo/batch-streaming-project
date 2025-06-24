USE DATABASE STREAMING_DB;

-- 1) RAW staging table
CREATE OR REPLACE TABLE RAW.transactions_raw (
  step             INTEGER,        -- Int64
  type             VARCHAR,        -- String
  amount           FLOAT,          -- Float64
  nameOrig         VARCHAR,        -- String
  oldbalanceOrg    FLOAT,          -- Float64
  newbalanceOrig   FLOAT,          -- Float64
  nameDest         VARCHAR,        -- String
  oldbalanceDest   FLOAT,          -- Float64
  newbalanceDest   FLOAT,          -- Float64
  isFraud          INTEGER,        -- Int64 (0 or 1)
  isFlaggedFraud   INTEGER         -- Int64 (0 or 1)
);

-- 2) PROCESSED fact table (same cols + load timestamp)
CREATE OR REPLACE TABLE PROCESSED.fct_transactions (
  time_step             INTEGER,
  transaction_type      VARCHAR,
  amount                FLOAT,
  name_orig             VARCHAR,
  old_balance_org       FLOAT,
  new_balance_orig      FLOAT,
  name_dest             VARCHAR,
  old_balance_dest      FLOAT,
  new_balance_dest      FLOAT,
  is_fraud              INTEGER,
  is_flagged_fraud      INTEGER,
  load_time             TIMESTAMP_LTZ   DEFAULT CURRENT_TIMESTAMP()
);