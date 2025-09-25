# scripts/03_merge_from_database.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark with Glue Catalog
spark = (SparkSession.builder
    .appName("Merge Hospital Data (Gold partitioned by state + CSV snapshot)")
    .config("spark.sql.catalogImplementation", "hive")
    .config("hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .getOrCreate())

# ---- Read Silver tables from Glue Catalog ----
print("🔹 Reading Silver tables from Glue Catalog...")
df_gen = (spark.read.table("hospital_readmissions.gen_info")
    .select(
        col("provider id").cast("long").alias("provider_id"),
        col("hospital ownership").alias("Hosp Ownership"),
        col("hospital overall rating").alias("Hosp Rating")
    ))

df_readm = (spark.read.table("hospital_readmissions.readmissions")
    .select(
        col("Provider ID").cast("long").alias("provider_id"),
        col("State").alias("state"),
        col("Measure Name").alias("Readm Type"),
        col("Number of Discharges").alias("Discharges"),
        col("Number of Readmissions").alias("Readmissions"),
        col("Excess Readmission Ratio").alias("Excess Readm Ratio"),
        col("Predicted Readmission Rate").alias("Pred Readm Rate"),
        col("Expected Readmission Rate").alias("Exp Readm Rate")
    ))

# ---- Merge & clean (Gold transform) ----
print("🔹 Joining and cleaning data (Gold)...")
df_merged = (df_readm.join(df_gen, on="provider_id", how="right")
    .replace("Not Available", None)
    .dropna(subset=["state", "Readm Type", "Pred Readm Rate", "Exp Readm Rate", "Hosp Ownership", "Hosp Rating"]))

readm_type_map = {
    "READM_30_PN_HRRP": "Pneumonia","READM_30_AMI_HRRP": "AMI",
    "READM_30_COPD_HRRP": "COPD","READM_30_CABG_HRRP": "CABG",
    "READM_30_HF_HRRP": "Heart_Failure","READM_30_HIP_KNEE_HRRP": "Hip_Knee"
}
ownership_map = {
    "Voluntary non-profit - Private": "Pvt NP","Proprietary": "Prop",
    "Voluntary non-profit - Other": "Other","Voluntary non-profit - Church": "Church",
    "Government - Hospital District or Authority": "Hospital","Government - Local": "Local",
    "Government - State": "State","Physician": "Physician","Government - Federal": "Federal",
    "Tribal": "Tribal"
}
rating_map = {"1": "1 Star","2": "2 Star","3": "3 Star","4": "4 Star","5": "5 Star"}

df_gold = (df_merged
    .replace(readm_type_map, subset=["Readm Type"])
    .replace(ownership_map, subset=["Hosp Ownership"])
    .replace(rating_map, subset=["Hosp Rating"])
    .withColumn("R Rate Diff", col("Pred Readm Rate").cast("float") - col("Exp Readm Rate").cast("float"))
    .select(
        "state", "provider_id", "Readm Type", "Discharges", "Readmissions",
        "Excess Readm Ratio", "Pred Readm Rate", "Exp Readm Rate",
        "Hosp Ownership", "Hosp Rating", "R Rate Diff"
    )
)

row_count = df_gold.count()
print(f"🔸 Gold DataFrame (pre-rename) has {row_count} rows.")

# ---- Write Gold Parquet (partitioned) BEFORE renaming to keep 'state' partition key ----
gold_parquet_path = "s3a://glue-hospital-data/athena/gold/merged/"
csv_snapshot_path = "s3a://glue-hospital-data/final_merged_output/"

if row_count > 0:
    print(f"📀 Writing GOLD Parquet (partitioned by state) to: {gold_parquet_path}")
    df_gold.write.mode("overwrite").partitionBy("state").parquet(gold_parquet_path)

    # --- Final friendly column names (single place to manage) ---
    rename_cols = {
        "state": "State",
        "provider_id": "Provider ID",
        "Readm Type": "Readmission Type",
        "Discharges": "Number of Discharges",
        "Readmissions": "Number of Readmissions",
        "Excess Readm Ratio": "Excess Readmission Ratio",
        "Pred Readm Rate": "Predicted Readmission Rate",
        "Exp Readm Rate": "Expected Readmission Rate",
        "Hosp Ownership": "Hospital Ownership",
        "Hosp Rating": "Hospital Overall Rating",
        "R Rate Diff": "Readmission Rate Difference",
    }

    df_csv = df_gold
    for old, new in rename_cols.items():
        df_csv = df_csv.withColumnRenamed(old, new)

    print("👀 Preview of final (friendly headers):")
    df_csv.show(10, truncate=False)

    print(f"📤 Writing presentation CSV snapshot to: {csv_snapshot_path}")
    df_csv.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_snapshot_path)
else:
    print("❌ No data to write.")

spark.stop()
