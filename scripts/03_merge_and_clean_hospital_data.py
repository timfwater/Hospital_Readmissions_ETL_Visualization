from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize Spark session with optimized S3 commit settings
spark = SparkSession.builder \
    .appName("MergeHospitalData") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.375") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Step 2: Load hospital general info
print("🔹 Reading hospital general info CSV...")
df_gen = spark.read.option("header", True).csv("s3a://glue-hospital-data/gen_info_recreated_csv_from_json/")
df_gen = df_gen.select(
    col("provider id").cast("long").alias("provider_id"),
    col("hospital ownership").alias("Hosp Ownership"),
    col("hospital overall rating").alias("Hosp Rating")
)

# Step 3: Load readmission info
print("🔹 Reading readmission CSV...")
df_readm = spark.read.option("header", True).csv("s3a://glue-hospital-data/readmission_recreated_csv_from_parquet/")
df_readm = df_readm.select(
    col("provider_id").cast("long").alias("provider_id"),
    col("state"),
    col("measure_name").alias("Readm Type"),
    col("number_of_discharges").alias("Discharges"),
    col("number_of_readmissions").alias("Readmissions"),
    col("excess_readmission_ratio").alias("Excess Readm Ratio"),
    col("predicted_readmission_rate").alias("Pred Readm Rate"),
    col("expected_readmission_rate").alias("Exp Readm Rate")
)

# Step 4: Merge and clean
print("🔹 Joining and cleaning data...")
df_merged = df_readm.join(df_gen, on="provider_id", how="right") \
    .replace("Not Available", None) \
    .dropna(subset=["Readm Type", "Pred Readm Rate", "Exp Readm Rate", "Hosp Ownership", "Hosp Rating"])

# Step 5: Apply mappings
readm_type_map = {
    "READM_30_PN_HRRP": "Pneumonia", "READM_30_AMI_HRRP": "AMI",
    "READM_30_COPD_HRRP": "COPD", "READM_30_CABG_HRRP": "CABG",
    "READM_30_HF_HRRP": "Heart_Failure", "READM_30_HIP_KNEE_HRRP": "Hip_Knee"
}
ownership_map = {
    "Voluntary non-profit - Private": "Pvt NP", "Proprietary": "Prop",
    "Voluntary non-profit - Other": "Other", "Voluntary non-profit - Church": "Church",
    "Government - Hospital District or Authority": "Hospital", "Government - Local": "Local",
    "Government - State": "State", "Physician": "Physician", "Government - Federal": "Federal",
    "Tribal": "Tribal"
}
rating_map = {
    "1": "1 Star", "2": "2 Star", "3": "3 Star", "4": "4 Star", "5": "5 Star"
}

df_merged = df_merged.replace(readm_type_map, subset=["Readm Type"]) \
    .replace(ownership_map, subset=["Hosp Ownership"]) \
    .replace(rating_map, subset=["Hosp Rating"]) \
    .withColumn("R Rate Diff", col("Pred Readm Rate").cast("float") - col("Exp Readm Rate").cast("float"))

# Step 6: Final selection
df_final = df_merged.select(
    "state", "provider_id", "Readm Type", "Discharges", "Readmissions",
    "Excess Readm Ratio", "Pred Readm Rate", "Exp Readm Rate",
    "Hosp Ownership", "Hosp Rating", "R Rate Diff"
)

# Step 7: Preview and write
row_count = df_final.count()
print(f"🔸 Final DataFrame has {row_count} rows.")
df_final.show(10, truncate=False)

if row_count > 0:
    output_path = "s3a://glue-hospital-data/final_merged_output/"
    print(f"📤 Writing output to: {output_path}")
    df_final.repartition(1).write.mode("overwrite").option("header", True).csv(output_path)
else:
    print("❌ No data to write.")

spark.stop()
