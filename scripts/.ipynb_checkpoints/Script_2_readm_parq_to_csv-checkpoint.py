from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadParquet") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.375") \
    .getOrCreate()

df = spark.read.parquet("s3a://glue-hospital-data/readmission_parquet/")

df_clean = df \
    .withColumnRenamed("hospital_name#0", "hospital_name") \
    .withColumnRenamed("provider_id#1", "provider_id") \
    .withColumnRenamed("measure_name#2", "measure_name") \
    .withColumnRenamed("number_of_discharges#3", "number_of_discharges") \
    .withColumnRenamed("excess_readmission_ratio#4", "excess_readmission_ratio") \
    .withColumnRenamed("predicted_readmission_rate#5", "predicted_readmission_rate") \
    .withColumnRenamed("expected_readmission_rate#6", "expected_readmission_rate") \
    .withColumnRenamed("number_of_readmissions#7", "number_of_readmissions") \
    .withColumnRenamed("start_date#8", "start_date") \
    .withColumnRenamed("end_date#9", "end_date")

df_clean.repartition(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("s3a://glue-hospital-data/readmission_recreated_csv_from_parquet/")

spark.stop()
