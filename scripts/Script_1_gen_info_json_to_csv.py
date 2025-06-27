from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Convert JSON to CSV") \
    .getOrCreate()

# Define schema explicitly for safety and consistency
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

# Read JSON from S3 (or local path)
df = spark.read.json("s3://glue-hospital-data/gen_info_json/", schema=schema)

# Optionally repartition to 1 file
df = df.repartition(1)

# Write as CSV to S3
df.write.mode("overwrite").option("header", True).csv("s3://glue-hospital-data/gen_info_recreated_csv_from_json/")

spark.stop()
