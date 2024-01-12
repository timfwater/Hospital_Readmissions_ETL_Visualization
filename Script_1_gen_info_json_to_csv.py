# Importing libraries
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Defining variables
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Referencing AWS Glue Data Catalog
AWSGlueDataCatalog_node1704035540938 = glueContext.create_dynamic_frame.from_catalog(
    database="hosp",
    table_name="gen_info_jsongen_info_json",
    transformation_ctx="AWSGlueDataCatalog_node1704035540938",
)

# Defining node Change Schema
ChangeSchema_node1704035547782 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1704035540938,
    mappings=[
        ("provider id", "int", "provider id", "int"),
        ("hospital name", "string", "hospital name", "string"),
        ("address", "string", "address", "string"),
        ("city", "string", "city", "string"),
        ("state", "string", "state", "string"),
        ("zip code", "int", "zip code", "int"),
        ("county name", "string", "county name", "string"),
        ("phone number", "long", "phone number", "long"),
        ("hospital type", "string", "hospital type", "string"),
        ("hospital ownership", "string", "hospital ownership", "string"),
        ("emergency services", "boolean", "emergency services", "boolean"),
        (
            "meets criteria for meaningful use of ehrs",
            "boolean",
            "meets criteria for meaningful use of ehrs",
            "boolean",
        ),
        ("hospital overall rating", "string", "hospital overall rating", "string"),
        (
            "hospital overall rating footnote",
            "string",
            "hospital overall rating footnote",
            "string",
        ),
        (
            "mortality national comparison",
            "string",
            "mortality national comparison",
            "string",
        ),
        (
            "mortality national comparison footnote",
            "string",
            "mortality national comparison footnote",
            "string",
        ),
        (
            "safety of care national comparison",
            "string",
            "safety of care national comparison",
            "string",
        ),
        (
            "safety of care national comparison footnote",
            "string",
            "safety of care national comparison footnote",
            "string",
        ),
        (
            "readmission national comparison",
            "string",
            "readmission national comparison",
            "string",
        ),
        (
            "readmission national comparison footnote",
            "string",
            "readmission national comparison footnote",
            "string",
        ),
        (
            "patient experience national comparison",
            "string",
            "patient experience national comparison",
            "string",
        ),
        (
            "patient experience national comparison footnote",
            "string",
            "patient experience national comparison footnote",
            "string",
        ),
        (
            "effectiveness of care national comparison",
            "string",
            "effectiveness of care national comparison",
            "string",
        ),
        (
            "effectiveness of care national comparison footnote",
            "string",
            "effectiveness of care national comparison footnote",
            "string",
        ),
        (
            "timeliness of care national comparison",
            "string",
            "timeliness of care national comparison",
            "string",
        ),
        (
            "timeliness of care national comparison footnote",
            "string",
            "timeliness of care national comparison footnote",
            "string",
        ),
        (
            "efficient use of medical imaging national comparison",
            "string",
            "efficient use of medical imaging national comparison",
            "string",
        ),
        (
            "efficient use of medical imaging national comparison footnote",
            "string",
            "efficient use of medical imaging national comparison footnote",
            "string",
        ),
        ("location", "string", "location", "string"),
    ],
    transformation_ctx="ChangeSchema_node1704035547782",
)

# Repartition the data to a single partition
repartitioned_frame = Repartition.apply(frame=ChangeSchema_node1704035547782, num_partitions=1, transformation_ctx="Repartition")

output_filename = "reformatted_general_information.csv"

# Exporting transformed file to S3 in CSV
AmazonS3_node1704035658887 = glueContext.write_dynamic_frame.from_options(
    frame=repartitioned_frame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://glue-hospital-data/gen_info_recreated_csv_from_json/",
        "FileName": output_filename
    },
    transformation_ctx="AmazonS3_node1704035658887",
)

job.commit()
