import polars as pl
import os

# Path setup, ensuring the dataset is in the correct location
file_path = os.path.join(os.path.dirname(__file__), '..', 'dataset', 'synthetic_financial_data.csv')
file_path = os.path.abspath(file_path)
df = pl.read_csv(file_path)

# Inspect structure
print("Shape:", df.shape)
print("Column names & dtypes:\n", df.dtypes)
print("\nSample rows:\n", df.head())

# Null/missing value check
nulls = df.null_count()
print("\nNull values per column:\n", nulls)

# Distribution: amount, type, isFraud
print("\nTransaction types:\n", df['type'].value_counts())
print("\nIsFraud counts:\n", df['isFraud'].value_counts())
print("\nAmount stats:\n", df['amount'].describe())

# Class imbalance
fraud_ratio = df['isFraud'].mean()
print(f"\nFraud ratio: {fraud_ratio:.4f} ({float(fraud_ratio)*100:.2f}%)")

# Balance changes
df = df.with_columns([
    (pl.col('newbalanceOrig') - pl.col('oldbalanceOrg')).alias('deltaOrig'),
    (pl.col('newbalanceDest') - pl.col('oldbalanceDest')).alias('deltaDest')
])
print("\nBalance delta (origin):\n", df['deltaOrig'].describe())
print("\nBalance delta (dest):\n", df['deltaDest'].describe())

# Time-based patterns
if 'step' in df.columns:
    print("\nStep (time) range:", df['step'].min(), "to", df['step'].max())
    fraud_by_time = (
    df.group_by('step')
      .agg(pl.col('isFraud').mean().alias('fraud_rate'))
      .sort('step')
    )
    print("\nFraud rate by step (sample):\n", fraud_by_time.head(10))

# Feature Engineering Suggestions
print("\nFeature Engineering Suggestions:")
print("- Transaction type (categorical: one-hot or embedding)")
print("- Log(amount + 1) (to reduce skew)")
print("- Balance change: deltaOrig, deltaDest, and ratios to amount")
print("- Frequency or sum of transactions per user in recent windows (needs aggregation)")
print("- Time features: hour, day, periodic patterns if available")
print("- Flags for impossible/negative balances")
print("- User history: previous fraud, amounts, types, etc.")
