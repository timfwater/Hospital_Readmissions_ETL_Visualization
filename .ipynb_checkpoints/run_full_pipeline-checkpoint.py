import boto3
import time

REGION = "us-east-1"
S3_SCRIPT_BUCKET = "s3://glue-hospital-data/scripts/"
SUBNET_ID = "subnet-9b61e5ba"
s3_output = "s3://glue-hospital-data/athena-query-results/"

emr_client = boto3.client("emr", region_name=REGION)
athena_client = boto3.client("athena", region_name=REGION)

def run_emr_job(script_path, job_name):
    print(f"🚀 Launching EMR job: {job_name}...")

    response = emr_client.run_job_flow(
        Name=job_name,
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}],
        Instances={
            "InstanceGroups": [
                {
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                }
            ],
            "KeepJobFlowAliveWhenNoSteps": False,
            "Ec2SubnetId": SUBNET_ID,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        VisibleToAllUsers=True,
        LogUri="s3://glue-hospital-data/emr-logs/",
        Steps=[
            {
                "Name": job_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        script_path
                    ]
                }
            }
        ],
        AutoTerminationPolicy={"IdleTimeout": 600},
    )

    cluster_id = response["JobFlowId"]
    print(f"⏳ Waiting for cluster {cluster_id} to complete...")

    waiter = emr_client.get_waiter("cluster_terminated")
    waiter.wait(ClusterId=cluster_id)

    print(f"✅ EMR job {job_name} completed.")

def refresh_athena_table(table_name):
    print(f"🔁 Refreshing Athena table: {table_name}")
    response = athena_client.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {table_name}",
        QueryExecutionContext={"Database": "hospital_readmissions"},
        ResultConfiguration={"OutputLocation": s3_output}
    )
    query_id = response["QueryExecutionId"]
    print(f"⏳ Waiting for Athena query {query_id} to complete...")

    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(3)

    if state != "SUCCEEDED":
        raise Exception(f"Athena query failed with state: {state}")
    print(f"✅ Athena table {table_name} refreshed.")

if __name__ == "__main__":
    try:
        # Step 1a: JSON → Parquet
        run_emr_job(
            script_path=S3_SCRIPT_BUCKET + "01_gen_info_json_to_parquet.py",
            job_name="Convert General Info JSON to Parquet"
        )

        # Step 1b: CSV → Parquet
        run_emr_job(
            script_path=S3_SCRIPT_BUCKET + "02_readmissions_csv_to_parquet.py",
            job_name="Convert Readmissions CSV to Parquet"
        )

        # Step 2: Refresh Athena
        refresh_athena_table("hospital_readmissions.gen_info")
        refresh_athena_table("hospital_readmissions.readmissions")

        # Step 3: Merge final output
        run_emr_job(
            script_path=S3_SCRIPT_BUCKET + "03_merge_from_database.py",
            job_name="Merge Final CSV Job"
        )

        print("🎉 Full pipeline completed successfully.")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
