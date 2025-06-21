import polars as pl
import os

pl.Config.set_tbl_cols(11)   # Default is 8 columns, show more for better understanding

# Path setup, ensuring the dataset is in the correct location
print("Loadiong dataset...")
file_path = os.path.join(os.path.dirname(__file__), '..', 'dataset', 'synthetic_financial_data.csv')
file_path = os.path.abspath(file_path)
df = pl.read_csv(file_path)

# Structure and basic info
print(f"Shape: {df.shape}")
print(f"Column names & data types:\n{df.columns}\n{df.dtypes}")
df = df.rename({'oldbalanceOrg': 'oldbalanceOrig'})
print(f"\nSample rows:\n {df.head(5)}")

# Null/missing value check
print(f"\nNull values per column:\n{df.null_count()}")

# Fraud and flag counts
print(f"\nIsFraud counts:\n{df['isFraud'].value_counts()}")
print(f"\nIsFlaggedFraud counts:\n{df['isFlaggedFraud'].value_counts()}") 
fraud_ratio = df['isFraud'].mean()
print(f"\nFraud ratio: {fraud_ratio:.4f} ({float(fraud_ratio)*100:.2f}%)")
flagged_fraud_ratio = df['isFlaggedFraud'].mean()
print(f"\nFlagged fraud ratio: {flagged_fraud_ratio:.4f} ({float(flagged_fraud_ratio)*100:.2f}%)")

# Fraud rate by step
if 'step' in df.columns:
    print(f"\nStep (time) range: {df['step'].min()} to {df['step'].max()}")
    fraud_by_time = (
    df.group_by('step')
      .agg(pl.col('isFraud').mean().alias('fraud_rate'))
      .sort('step')
    )
    print(f"\nSample of fraud rate by step:\n{fraud_by_time.head(10)}")

# Balance changes for origin and destination
df = df.with_columns([
    (pl.col('newbalanceOrig') - pl.col('oldbalanceOrig')).alias('deltaOrig'),
    (pl.col('newbalanceDest') - pl.col('oldbalanceDest')).alias('deltaDest')
])