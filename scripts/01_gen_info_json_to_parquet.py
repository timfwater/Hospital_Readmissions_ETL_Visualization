from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType
import sys

# ✅ Initialize Spark session with S3 support and correct timeout config
spark = SparkSession.builder \
    .appName("GeneralInfoJSONtoParquet") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars", "jars/hadoop-aws-3.3.2.jar,jars/aws-java-sdk-bundle-1.12.375.jar") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    .getOrCreate()

# ✅ Schema for the JSON structure
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

# ✅ S3 paths
input_path = "s3a://glue-hospital-data/gen_info_json/"
output_path = "s3a://glue-hospital-data/athena/gen_info/"

# ✅ Load JSON
df = spark.read.json(input_path, schema=schema)

# ✅ Validate
count = df.count()
if count == 0:
    print(f"❌ No records found at: {input_path}")
    sys.exit(1)
else:
    print(f"✅ Loaded {count} records from: {input_path}")

# ✅ Write as Parquet
df.repartition(1).write.mode("overwrite").parquet(output_path)
print(f"✅ Parquet written to: {output_path}")

spark.stop()
