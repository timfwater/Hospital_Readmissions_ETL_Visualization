from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Start Spark
spark = SparkSession.builder.appName("MergeHospitalData").getOrCreate()

# Load hospital general info (CSV created from JSON)
df_gen = spark.read.option("header", True).csv("s3://glue-hospital-data/gen_info_recreated_csv_from_json/")
df_gen = df_gen.select(
    col("provider id").cast("long").alias("provider_id"),
    col("hospital ownership").alias("Hosp Ownership"),
    col("hospital overall rating").alias("Hosp Rating")
)

# Load readmission info (CSV created from Parquet)
df_readm = spark.read.option("header", True).csv("s3://glue-hospital-data/readmission_recreated_csv_from_parquet/")
df_readm = df_readm.select(
    col("provider_id#1").cast("long").alias("provider_id"),
    col("state"),
    col("measure_name#2").alias("Readm Type"),
    col("number_of_discharges#3").alias("Discharges"),
    col("number_of_readmissions#7").alias("Readmissions"),
    col("excess_readmission_ratio#4").alias("Excess Readm Ratio"),
    col("predicted_readmission_rate#5").alias("Pred Readm Rate"),
    col("expected_readmission_rate#6").alias("Exp Readm Rate")
)

# Merge on provider_id
df_merged = df_readm.join(df_gen, on="provider_id", how="right")

# Replace "Not Available" with nulls and drop any rows with missing data in key columns
df_merged = df_merged.replace("Not Available", None)
df_merged = df_merged.dropna(subset=["Readm Type", "Pred Readm Rate", "Exp Readm Rate", "Hosp Ownership", "Hosp Rating"])

# Rename values for display (readmission type, ownership, rating)
readm_type_map = {
    "READM_30_PN_HRRP": "Pneumonia",
    "READM_30_AMI_HRRP": "AMI",
    "READM_30_COPD_HRRP": "COPD",
    "READM_30_CABG_HRRP": "CABG",
    "READM_30_HF_HRRP": "Heart_Failure",
    "READM_30_HIP_KNEE_HRRP": "Hip_Knee"
}
ownership_map = {
    "Voluntary non-profit - Private": "Pvt NP",
    "Proprietary": "Prop",
    "Voluntary non-profit - Other": "Other",
    "Voluntary non-profit - Church": "Church",
    "Government - Hospital District or Authority": "Hospital",
    "Government - Local": "Local",
    "Government - State": "State",
    "Physician": "Physician",
    "Government - Federal": "Federal",
    "Tribal": "Tribal"
}
rating_map = {
    "1": "1 Star", "2": "2 Star", "3": "3 Star", "4": "4 Star", "5": "5 Star"
}

df_merged = df_merged.replace(readm_type_map, subset=["Readm Type"])
df_merged = df_merged.replace(ownership_map, subset=["Hosp Ownership"])
df_merged = df_merged.replace(rating_map, subset=["Hosp Rating"])

# Calculate difference between predicted and expected readmission rate
df_merged = df_merged.withColumn("R Rate Diff",
    col("Pred Readm Rate").cast("float") - col("Exp Readm Rate").cast("float")
)

# Final column selection
df_final = df_merged.select(
    "state", "provider_id", "Readm Type", "Discharges", "Readmissions",
    "Excess Readm Ratio", "Pred Readm Rate", "Exp Readm Rate",
    "Hosp Ownership", "Hosp Rating", "R Rate Diff"
)

# Write result to S3
df_final.repartition(1).write.mode("overwrite").option("header", True).csv("s3://glue-hospital-data/final_merged_output/")

spark.stop()
