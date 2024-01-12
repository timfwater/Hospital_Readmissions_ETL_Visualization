# Importing libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Defining variables
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Referencing AWS Glue Data Catalog for Hospital data
AWSGlueDataCatalog_node1704043413523 = glueContext.create_dynamic_frame.from_catalog(
    database="hosp",
    table_name="reformated_gengen_info_recreated_csv_from_json",
    transformation_ctx="AWSGlueDataCatalog_node1704043413523",
)

# Referencing AWS Glue Data Catalog for Readmissions data
AWSGlueDataCatalog_node1704043414951 = glueContext.create_dynamic_frame.from_catalog(
    database="hosp",
    table_name="readm_recreatereadmission_recreated_csv_from_parquet",
    transformation_ctx="AWSGlueDataCatalog_node1704043414951",
)

# Defining our Hospital input columns
ChangeSchemagen_node1704043486632 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1704043413523,
    mappings=[
        ("provider id", "long", "provider id", "long"),
        ("hospital ownership", "string", "Hosp Ownership", "string"),
        ("hospital overall rating", "string", "Hosp Rating", "string"),
    ],
    transformation_ctx="ChangeSchemagen_node1704043486632",
)

# Defining our Readmissions input columns
ChangeSchemareadm_node1704043418499 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1704043414951,
    mappings=[
        ("provider_id#1", "long", "provider_id_r", "long"),
        ("state", "string", "state", "string"),
        ("measure_name#2", "string", "Readm Type", "string"),
        ("number_of_discharges#3", "string", "Discharges", "string"),
        ("excess_readmission_ratio#4", "string", "Excess Readm Ratio", "string"),
        ("predicted_readmission_rate#5", "string", "Pred Readm Rate", "string"),
        ("expected_readmission_rate#6", "string", "Exp Readm Rate", "string"),
        ("number_of_readmissions#7", "string", "Readmissions", "string"),
    ],
    transformation_ctx="ChangeSchemareadm_node1704043418499",
)

# Defining node Join
ChangeSchemareadm_node1704043418499DF = ChangeSchemareadm_node1704043418499.toDF()
ChangeSchemagen_node1704043486632DF = ChangeSchemagen_node1704043486632.toDF()

# Join on provider ID
Join_node1704043499047 = DynamicFrame.fromDF(
    ChangeSchemareadm_node1704043418499DF.join(
        ChangeSchemagen_node1704043486632DF,
        (
            ChangeSchemareadm_node1704043418499DF["provider_id_r"]
            == ChangeSchemagen_node1704043486632DF["provider id"]
        ),
        "right"
    ).select(
        ChangeSchemareadm_node1704043418499DF["state"],
        ChangeSchemagen_node1704043486632DF["provider id"],  # Keep only the "provider id from Gen Info
        ChangeSchemareadm_node1704043418499DF["Readm Type"],
        ChangeSchemareadm_node1704043418499DF["Discharges"],
        ChangeSchemareadm_node1704043418499DF["Readmissions"],
        ChangeSchemareadm_node1704043418499DF["Excess Readm Ratio"],
        ChangeSchemareadm_node1704043418499DF["Pred Readm Rate"],
        ChangeSchemareadm_node1704043418499DF["Exp Readm Rate"],
        ChangeSchemagen_node1704043486632DF["Hosp Ownership"],
        ChangeSchemagen_node1704043486632DF["Hosp Rating"],
    ),
    glueContext,
    "Join_node1704043499047"
)

# Define the desired order of columns
column_order = [
    "state",
    "provider id",
    "Readm Type",
    "Discharges",
    "Readmissions",
    "Excess Readm Ratio",
    "Pred Readm Rate",
    "Exp Readm Rate",
    "Hosp Ownership",
    "Hosp Rating",
    "R Rate Diff",  # New column -- difference betweem Exp/Pred reamdmission rates
]

# Select columns in the desired order
Join_node1704043499047 = Join_node1704043499047.select_fields(column_order)

# Convert "Not Available" to None before using Filter
Join_node1704043499047 = Join_node1704043499047.map(
    transformation_ctx="Replace_Not_Available",
    f=lambda x: dict((k, None) if v == "Not Available" else (k, v) for k, v in x.items())
)

# Filtering out rows with null values
Join_node1704043499047 = Filter.apply(
    frame=Join_node1704043499047,
    f=lambda x: all(value is not None for value in x.values())
)

# Convert the DataFrame back to a DynamicFrame
Join_node1704043499047 = DynamicFrame.fromDF(
    Join_node1704043499047.toDF(),
    glueContext,
    "Join_node1704043499047"
)

# Value replacement for Readmission Type column
measure_value_mapping = {
    "READM_30_PN_HRRP": "Pneumonia",
    "READM_30_AMI_HRRP": "AMI",
    "READM_30_COPD_HRRP": "COPD",
    "READM_30_CABG_HRRP": "CABG",
    "READM_30_HF_HRRP": "Heart_Failure",
    "READM_30_HIP_KNEE_HRRP": "Hip_Knee"
}

# Replace values in DynamicFrame
for old_value, new_value in measure_value_mapping.items():
    Join_node1704043499047 = Join_node1704043499047.map(
        transformation_ctx="ReplaceValues_" + old_value,
        f=lambda x: x if x["Readm Type"] != old_value else dict(x, **{"Readm Type": new_value})
    )

# Value replacement for the Hospital Ownership column
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

# Replace values in DynamicFrame
for old_value, new_value in hosp_own_value_mapping.items():
    Join_node1704043499047 = Join_node1704043499047.map(
        transformation_ctx="ReplaceValues_" + old_value,
        f=lambda x: x if x["Hosp Ownership"] != old_value else dict(x, **{"Hosp Ownership": new_value})
    )

# Value replacement for the Hospital Ownership column
hosp_rating_value_mapping = {
    1: "1 Star",
    2: "2 Star",
    3: "3 Star", 
    4: "4 Star",
    5: "5 Star"
}

# Replace values in DynamicFrame
for old_value, new_value in hosp_rating_value_mapping.items():
    Join_node1704043499047 = Join_node1704043499047.map(
        transformation_ctx="ReplaceValues_" + str(old_value),
        f=lambda x: dict(x, **{"Hosp Rating": new_value}) 
        if x["Hosp Rating"] == str(old_value) else x
    )

#Creating our new custom KPI
Join_node1704043499047 = Join_node1704043499047.map(
    transformation_ctx="Calculate_Difference",
    f=lambda x: dict(x, **{"R Rate Diff": float(x["Pred Readm Rate"]) - float(x["Exp Readm Rate"])})
)

# Repartition the data to a single partition
repartitioned_frame = Repartition.apply(frame=Join_node1704043499047, num_partitions=1, transformation_ctx="Repartition")

# Specify the desired filename
output_filename = "merged_engineered_hospital.csv"

# Script generated for node Amazon S3
AmazonS3_node1704043614156 = glueContext.write_dynamic_frame.from_options(
    frame=repartitioned_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://glue-hospital-data/final_merged_output/",
        "FileName": output_filename
    },
    transformation_ctx="AmazonS3_node1704043614156",
)

job.commit()