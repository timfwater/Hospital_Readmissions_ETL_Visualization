import os
import sys
import time
import subprocess
import boto3
import traceback

# -------------------------
# Configuration (from env with sensible defaults)
# -------------------------
REGION = os.getenv("AWS_REGION", "us-east-1")
SCRIPT_BUCKET = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")  # bucket name only
S3_SCRIPT_PREFIX = "scripts"
S3_SCRIPT_BUCKET = f"s3://{SCRIPT_BUCKET}/{S3_SCRIPT_PREFIX}/"

SUBNET_ID = os.getenv("SUBNET_ID", "subnet-9b61e5ba")
ATHENA_DB = os.getenv("ATHENA_DB", "hospital_readmissions")
ATHENA_OUTPUT_PREFIX = os.getenv("ATHENA_OUTPUT_PREFIX", "athena-query-results")
S3_OUTPUT = f"s3://{SCRIPT_BUCKET}/{ATHENA_OUTPUT_PREFIX}/"

LAUNCH_STREAMLIT = os.getenv("LAUNCH_STREAMLIT", "false").lower() == "true"

# -------------------------
# AWS Clients
# -------------------------
emr_client = boto3.client("emr", region_name=REGION)
athena_client = boto3.client("athena", region_name=REGION)

def log(msg: str):
    print(msg, flush=True)

# -------------------------
# EMR Job Runner
# -------------------------
def run_emr_job(script_path: str, job_name: str):
    log(f"🚀 Launching EMR job: {job_name}")
    response = emr_client.run_job_flow(
        Name=job_name,
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}],
        Instances={
            "InstanceGroups": [
                {"InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
                {"InstanceRole": "CORE",   "InstanceType": "m5.xlarge", "InstanceCount": 1}
            ],
            "KeepJobFlowAliveWhenNoSteps": False,
            "Ec2SubnetId": SUBNET_ID,
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        VisibleToAllUsers=True,
        LogUri=f"s3://{SCRIPT_BUCKET}/emr-logs/",
        Steps=[
            {
                "Name": job_name,
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": ["spark-submit", "--deploy-mode", "cluster", script_path],
                },
            }
        ],
        AutoTerminationPolicy={"IdleTimeout": 600},
    )

    cluster_id = response["JobFlowId"]
    log(f"⏳ Waiting for EMR cluster {cluster_id} to terminate (job complete)...")

    waiter = emr_client.get_waiter("cluster_terminated")
    waiter.wait(ClusterId=cluster_id)

    # optional: sanity check final state
    desc = emr_client.describe_cluster(ClusterId=cluster_id)
    state = desc["Cluster"]["Status"]["State"]
    log(f"✅ EMR cluster {cluster_id} state: {state} for job '{job_name}'")

# -------------------------
# Athena Table Refresher
# -------------------------
def refresh_athena_table(table_name: str):
    log(f"🔁 Refreshing Athena table: {table_name}")
    response = athena_client.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {table_name}",
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    query_id = response["QueryExecutionId"]
    log(f"⏳ Waiting for Athena query {query_id} to complete...")

    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(3)

    if state == "SUCCEEDED":
        log(f"✅ Athena table refreshed: {table_name}")
    else:
        raise RuntimeError(f"❌ Athena query failed: {query_id} (State: {state})")

# -------------------------
# Main Pipeline Execution
# -------------------------
if __name__ == "__main__":
    try:
        log("🚀 Starting Hospital Readmissions ETL pipeline...")
        log(f"ENV: REGION={REGION}  SCRIPT_BUCKET={SCRIPT_BUCKET}  SUBNET_ID={SUBNET_ID}")
        log(f"ENV: ATHENA_DB={ATHENA_DB}  S3_OUTPUT={S3_OUTPUT}  LAUNCH_STREAMLIT={LAUNCH_STREAMLIT}")
        log(f"PYTHON: {sys.version.split()[0]}  CWD: {os.getcwd()}")

        # Step 1a: Convert general info JSON → Parquet
        run_emr_job(
            script_path=S3_SCRIPT_BUCKET + "01_gen_info_json_to_parquet.py",
            job_name="Convert General Info to Parquet"
        )

        # Step 1b: Convert readmissions CSV → Parquet
        run_emr_job(
            script_path=S3_SCRIPT_BUCKET + "02_readmissions_csv_to_parquet.py",
            job_name="Convert Readmissions to Parquet"
        )

        # Step 2: Refresh Athena tables
        refresh_athena_table("hospital_readmissions.gen_info")
        refresh_athena_table("hospital_readmissions.readmissions")

        # Step 3: Merge Parquet data → final CSV
        run_emr_job(
            script_path=S3_SCRIPT_BUCKET + "03_merge_from_database.py",
            job_name="Merge Final Output to CSV"
        )

        log("\n🎉 FULL PIPELINE COMPLETED SUCCESSFULLY.")

        # Step 4 (optional in Fargate): Launch Streamlit dashboard
        if LAUNCH_STREAMLIT:
            log("\n🌐 Launching Streamlit dashboard on port 8501...")
            subprocess.run(["streamlit", "run", "streamlit/streamlit_app.py"], check=False)

    except Exception as e:
        log("\n❌ PIPELINE FAILED")
        log(str(e))
        traceback.print_exc()
        sys.exit(1)
