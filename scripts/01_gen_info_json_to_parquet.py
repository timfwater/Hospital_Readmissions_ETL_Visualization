# scripts/01_gen_info_json_to_parquet.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
import os
import sys

# ---------- Config (env with safe defaults) ----------
BUCKET = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")
INPUT_PREFIX = os.getenv("GEN_INFO_INPUT_PREFIX", "gen_info_json/")
OUTPUT_PREFIX = os.getenv("GEN_INFO_OUTPUT_PREFIX", "athena/gen_info/")

input_path = f"s3a://{BUCKET}/{INPUT_PREFIX}"
output_path = f"s3a://{BUCKET}/{OUTPUT_PREFIX}"

# ---------- Spark (EMR-friendly: no local JAR paths) ----------
spark = (
    SparkSession.builder
    .appName("GeneralInfoJSONtoParquet")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .getOrCreate()
)

# ---------- Schema ----------
schema = StructType([
    StructField("provider id", IntegerType(), True),
    StructField("hospital name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip code", IntegerType(), True),
    StructField("county name", StringType(), True),
    StructField("phone number", LongType(), True),
    StructField("hospital type", StringType(), True),
    StructField("hospital ownership", StringType(), True),
    StructField("emergency services", BooleanType(), True),
    StructField("meets criteria for meaningful use of ehrs", BooleanType(), True),
    StructField("hospital overall rating", StringType(), True),
    StructField("hospital overall rating footnote", StringType(), True),
    StructField("mortality national comparison", StringType(), True),
    StructField("mortality national comparison footnote", StringType(), True),
    StructField("safety of care national comparison", StringType(), True),
    StructField("safety of care national comparison footnote", StringType(), True),
    StructField("readmission national comparison", StringType(), True),
    StructField("readmission national comparison footnote", StringType(), True),
    StructField("patient experience national comparison", StringType(), True),
    StructField("patient experience national comparison footnote", StringType(), True),
    StructField("effectiveness of care national comparison", StringType(), True),
    StructField("effectiveness of care national comparison footnote", StringType(), True),
    StructField("timeliness of care national comparison", StringType(), True),
    StructField("timeliness of care national comparison footnote", StringType(), True),
    StructField("efficient use of medical imaging national comparison", StringType(), True),
    StructField("efficient use of medical imaging national comparison footnote", StringType(), True),
    StructField("location", StringType(), True)
])

# ---------- Read ----------
df = spark.read.json(input_path, schema=schema)
count = df.count()
if count == 0:
    print(f"❌ No records found at: {input_path}")
    spark.stop()
    sys.exit(1)
print(f"✅ Loaded {count} records from: {input_path}")

# ---------- Write (Silver Parquet; scalable: no single-file coercion) ----------
df.write.mode("overwrite").parquet(output_path)
print(f"✅ Silver Parquet written to: {output_path}")

spark.stop()
