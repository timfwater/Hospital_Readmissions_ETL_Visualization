from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Step 1: Initialize Spark session with Glue Catalog integration
spark = SparkSession.builder \
    .appName("Merge Hospital Data") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .getOrCreate()

# Step 2: Load hospital general info from Glue Catalog
print("🔹 Reading hospital general info from Glue Catalog...")
df_gen = spark.read.table("hospital_readmissions.gen_info")
df_gen = df_gen.select(
    col("provider id").cast("long").alias("provider_id"),
    col("hospital ownership").alias("Hosp Ownership"),
    col("hospital overall rating").alias("Hosp Rating")
)

# Step 3: Load readmission info from Glue Catalog
print("🔹 Reading readmission info from Glue Catalog...")
df_readm = spark.read.table("hospital_readmissions.readmissions")
df_readm = df_readm.select(
    col("Provider ID").cast("long").alias("provider_id"),
    col("State").alias("state"),
    col("Measure Name").alias("Readm Type"),
    col("Number of Discharges").alias("Discharges"),
    col("Number of Readmissions").alias("Readmissions"),
    col("Excess Readmission Ratio").alias("Excess Readm Ratio"),
    col("Predicted Readmission Rate").alias("Pred Readm Rate"),
    col("Expected Readmission Rate").alias("Exp Readm Rate")
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
