import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1704041557520 = glueContext.create_dynamic_frame.from_catalog(
    database="hosp",
    table_name="readm_parqreadmission_parquet",
    transformation_ctx="AWSGlueDataCatalog_node1704041557520",
)

# Script generated for node Change Schema
ChangeSchema_node1704041562743 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1704041557520,
    mappings=[
        ("hospital_name#0", "string", "hospital_name#0", "string"),
        ("provider_id#1", "long", "provider_id#1", "long"),
        ("state", "string", "state", "string"),
        ("measure_name#2", "string", "measure_name#2", "string"),
        ("number_of_discharges#3", "string", "number_of_discharges#3", "string"),
        ("footnote", "string", "footnote", "string"),
        (
            "excess_readmission_ratio#4",
            "string",
            "excess_readmission_ratio#4",
            "string",
        ),
        (
            "predicted_readmission_rate#5",
            "string",
            "predicted_readmission_rate#5",
            "string",
        ),
        (
            "expected_readmission_rate#6",
            "string",
            "expected_readmission_rate#6",
            "string",
        ),
        ("number_of_readmissions#7", "string", "number_of_readmissions#7", "string"),
        ("start_date#8", "string", "start_date#8", "string"),
        ("end_date#9", "string", "end_date#9", "string"),
    ],
    transformation_ctx="ChangeSchema_node1704041562743",
)

# Repartition the data to a single partition
repartitioned_frame = Repartition.apply(frame=ChangeSchema_node1704041562743, num_partitions=1, transformation_ctx="Repartition")

output_filename = "reformatted_readmission_information.csv"

# Script generated for node Amazon S3
AmazonS3_node1704041564805 = glueContext.write_dynamic_frame.from_options(
    frame=repartitioned_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://glue-hospital-data/readmission_recreated_csv_from_parquet/",
        "FileName": output_filename
    },
    transformation_ctx="AmazonS3_node1704041564805",
)

job.commit()
