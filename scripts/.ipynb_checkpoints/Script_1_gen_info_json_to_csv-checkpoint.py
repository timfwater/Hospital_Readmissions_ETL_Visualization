from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType

# Initialize Spark session (no fancy commit protocol config)
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.375") \
    .getOrCreate()

# Define schema
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

# ✅ Read JSON from S3
df = spark.read.json("s3a://glue-hospital-data/gen_info_json/", schema=schema)

# ✅ Write CSV to S3
df.repartition(1).write.mode("overwrite").option("header", True).csv("s3a://glue-hospital-data/gen_info_recreated_csv_from_json/")

spark.stop()
