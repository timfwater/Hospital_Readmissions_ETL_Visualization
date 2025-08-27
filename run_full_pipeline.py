# run_full_pipeline.py
import os
import sys
import time
import subprocess
import traceback
import pathlib
import argparse
import boto3

# -------------------------
# Configuration (env with sensible defaults)
# -------------------------
REGION = os.getenv("AWS_REGION", "us-east-1")
SCRIPT_BUCKET = os.getenv("SCRIPT_BUCKET", "glue-hospital-data")
S3_SCRIPT_PREFIX = os.getenv("S3_SCRIPT_PREFIX", "scripts")
S3_SCRIPT_BUCKET = f"s3://{SCRIPT_BUCKET}/{S3_SCRIPT_PREFIX}/"

SUBNET_ID = os.getenv("SUBNET_ID", "subnet-9b61e5ba")
ATHENA_DB = os.getenv("ATHENA_DB", "hospital_readmissions")
ATHENA_OUTPUT_PREFIX = os.getenv("ATHENA_OUTPUT_PREFIX", "athena-query-results")
S3_OUTPUT = f"s3://{SCRIPT_BUCKET}/{ATHENA_OUTPUT_PREFIX}/"

LAUNCH_STREAMLIT_ENV = os.getenv("LAUNCH_STREAMLIT", "false").lower() == "true"
RUN_MODE = os.getenv("RUN_MODE", "emr").lower()  # 'emr' (default) or 'local'

# -------------------------
# AWS Clients
# -------------------------
emr_client = boto3.client("emr", region_name=REGION)
athena_client = boto3.client("athena", region_name=REGION)
s3_client = boto3.client("s3", region_name=REGION)

def log(msg: str):
    print(msg, flush=True)

# -------------------------
# Sync local scripts → S3 (so EMR can read them)
# -------------------------
def sync_scripts_to_s3(local_dir="scripts", bucket=None, prefix="scripts"):
    bucket = bucket or SCRIPT_BUCKET
    base = pathlib.Path(local_dir)
    log(f"🆙 Syncing {base}/ → s3://{bucket}/{prefix}/")
    uploaded = 0
    for p in base.glob("*.py"):
        key = f"{prefix}/{p.name}"
        s3_client.upload_file(str(p), bucket, key)
        uploaded += 1
        log(f"   • {p.name} → s3://{bucket}/{key}")
    if uploaded == 0:
        log("⚠️ No .py scripts found to sync.")

# -------------------------
# EMR / Local Spark runners
# -------------------------
def run_emr_job(script_s3: str, job_name: str):
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
        Steps=[{
            "Name": job_name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", "--deploy-mode", "cluster", script_s3],
            },
        }],
        AutoTerminationPolicy={"IdleTimeout": 600},
    )
    cluster_id = response["JobFlowId"]
    log(f"⏳ Waiting for EMR cluster {cluster_id} to terminate...")
    waiter = emr_client.get_waiter("cluster_terminated")
    waiter.wait(ClusterId=cluster_id)
    state = emr_client.describe_cluster(ClusterId=cluster_id)["Cluster"]["Status"]["State"]
    log(f"✅ EMR cluster {cluster_id} state: {state} for job '{job_name}'")

def run_local_spark(local_script: str, job_name: str):
    # For dev/testing: requires local Spark + s3a jars properly installed
    log(f"🚀 (local Spark) {job_name}: {local_script}")
    subprocess.check_call(f"spark-submit {local_script}", shell=True)

def run_spark(script_name: str, job_name: str):
    if RUN_MODE == "local":
        run_local_spark(f"scripts/{script_name}", job_name)
    else:
        run_emr_job(S3_SCRIPT_BUCKET + script_name, job_name)

# -------------------------
# Athena helpers
# -------------------------
def poke_athena_table(table_name: str):
    log(f"🔎 Poking Athena table: {table_name}")
    resp = athena_client.start_query_execution(
        QueryString=f"SELECT count(1) AS c FROM {table_name} LIMIT 1",
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    while True:
        res = athena_client.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)
    if state != "SUCCEEDED":
        raise RuntimeError(f"❌ Athena poke failed for {table_name} (Query {qid}, State {state})")
    log(f"✅ Athena table reachable: {table_name}")

def msck_repair(table_name: str):
    log(f"🧩 Repairing partitions for {table_name}")
    resp = athena_client.start_query_execution(
        QueryString=f"MSCK REPAIR TABLE {table_name}",
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    while True:
        res = athena_client.get_query_execution(QueryExecutionId=qid)
        state = res["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)
    if state != "SUCCEEDED":
        raise RuntimeError(f"❌ MSCK REPAIR failed for {table_name} (Query {qid}, State {state})")
    log(f"✅ Partitions repaired for {table_name}")

# -------------------------
# Streamlit launcher
# -------------------------
def launch_streamlit():
    log("\n🌐 Launching Streamlit dashboard on port 8501...")
    subprocess.run(["streamlit", "run", "streamlit/streamlit_app.py"], check=False)

# -------------------------
# Pipeline stages
# -------------------------
def stage_sync():
    sync_scripts_to_s3(local_dir="scripts", bucket=SCRIPT_BUCKET, prefix=S3_SCRIPT_PREFIX)

def stage_silver():
    run_spark("01_gen_info_json_to_parquet.py", "Convert General Info to Parquet")
    run_spark("02_readmissions_csv_to_parquet.py", "Convert Readmissions to Parquet")

def stage_athena_poke():
    poke_athena_table("hospital_readmissions.gen_info")
    poke_athena_table("hospital_readmissions.readmissions")

def stage_gold_and_csv():
    run_spark("03_merge_from_database.py", "Merge Final Output to CSV (and Gold Parquet)")
    # New: register partition metadata for Gold
    msck_repair("hospital_readmissions.gold_merged")

def stage_app():
    launch_streamlit()

# -------------------------
# CLI
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Hospital Readmissions ETL Orchestrator (Fargate→EMR→Athena→Streamlit)"
    )
    sub = p.add_subparsers(dest="cmd")

    sub.add_parser("full", help="Run sync → Silver → Athena poke → Gold+CSV (+partition repair) (default)")
    sub.add_parser("silver", help="Run Silver steps only (Parquet for gen_info/readmissions)")
    sub.add_parser("athena", help="Run Athena reachability checks only")
    sub.add_parser("gold", help="Run Gold merge + CSV snapshot only (also repairs partitions)")
    sub.add_parser("app", help="Run Streamlit app locally")
    sub.add_parser("sync", help="Upload scripts/*.py to S3 only")

    return p.parse_args()

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    try:
        args = parse_args()
        cmd = args.cmd or "full"

        log("🚀 Starting Hospital Readmissions ETL")
        log(f"ENV  REGION={REGION}  SCRIPT_BUCKET={SCRIPT_BUCKET}  SUBNET_ID={SUBNET_ID}")
        log(f"ENV  ATHENA_DB={ATHENA_DB}  S3_OUTPUT={S3_OUTPUT}  RUN_MODE={RUN_MODE}")
        log(f"PY   {sys.version.split()[0]}  CWD={os.getcwd()}")
        if LAUNCH_STREAMLIT_ENV and cmd == "full":
            log("ℹ️  LAUNCH_STREAMLIT=true detected (will launch after pipeline).")

        if cmd == "full":
            stage_sync()
            stage_silver()
            stage_athena_poke()
            stage_gold_and_csv()
            log("\n🎉 FULL PIPELINE COMPLETED SUCCESSFULLY.")
            if LAUNCH_STREAMLIT_ENV:
                stage_app()

        elif cmd == "silver":
            stage_sync()
            stage_silver()
            log("\n✅ Silver completed.")

        elif cmd == "athena":
            stage_athena_poke()
            log("\n✅ Athena checks completed.")

        elif cmd == "gold":
            stage_sync()
            stage_gold_and_csv()
            log("\n✅ Gold + CSV completed (partitions repaired).")

        elif cmd == "app":
            stage_app()

        elif cmd == "sync":
            stage_sync()
            log("\n✅ Scripts synced to S3.")

        else:
            raise SystemExit(f"Unknown command: {cmd}")

    except Exception as e:
        log("\n❌ PIPELINE FAILED")
        log(str(e))
        traceback.print_exc()
        sys.exit(1)
