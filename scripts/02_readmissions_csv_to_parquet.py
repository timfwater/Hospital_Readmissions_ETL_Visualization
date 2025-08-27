# scripts/02_readmissions_csv_to_parquet.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# ---------- Config ----------
BUCKET = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")
INPUT_PREFIX = os.getenv("READM_INPUT_PREFIX", "Readmissions_Reduction/")
OUTPUT_PREFIX = os.getenv("READM_OUTPUT_PREFIX", "athena/readmissions/")

input_path = f"s3a://{BUCKET}/{INPUT_PREFIX}"
output_path = f"s3a://{BUCKET}/{OUTPUT_PREFIX}"

# ---------- Spark ----------
spark = (
    SparkSession.builder
    .appName("ConvertReadmCSVToParquet")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)

# ---------- Read raw CSV ----------
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)  # ok for this stage; you can hardcode schema later if you prefer
      .csv(input_path))

# ---------- Normalize column names if needed ----------
# Your sample showed names like "hospital_name#0" etc. Handle both cases.
rename_map = {
    "hospital_name#0": "Hospital Name",
    "provider_id#1": "Provider ID",
    "measure_name#2": "Measure Name",
    "number_of_discharges#3": "Number of Discharges",
    "excess_readmission_ratio#4": "Excess Readmission Ratio",
    "predicted_readmission_rate#5": "Predicted Readmission Rate",
    "expected_readmission_rate#6": "Expected Readmission Rate",
    "number_of_readmissions#7": "Number of Readmissions",
    "start_date#8": "Start Date",
    "end_date#9": "End Date",
}

for old, new in rename_map.items():
    if old in df.columns and new not in df.columns:
        df = df.withColumnRenamed(old, new)

# Optional: ensure the canonical columns exist (light touch—safe even if already correct)
canonical = [
    "Hospital Name", "Provider ID", "State", "Measure Name",
    "Number of Discharges", "Footnote", "Excess Readmission Ratio",
    "Predicted Readmission Rate", "Expected Readmission Rate",
    "Number of Readmissions", "Start Date", "End Date",
]
missing = [c for c in canonical if c not in df.columns]
if missing:
    print(f"⚠️  Missing expected columns (continuing): {missing}")

# ---------- Write (Silver Parquet) ----------
df.write.mode("overwrite").parquet(output_path)
print(f"✅ Silver Parquet written to: {output_path}")

spark.stop()
