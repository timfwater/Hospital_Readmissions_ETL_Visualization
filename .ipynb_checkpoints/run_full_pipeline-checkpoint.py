import boto3
import subprocess
import time
import json

# Correct endpoint for EMR and Athena clients
emr_client = boto3.client("emr", region_name="us-east-1", endpoint_url="https://elasticmapreduce.us-east-1.amazonaws.com")
athena_client = boto3.client("athena", region_name="us-east-1")
s3_output = "s3://glue-hospital-data/athena-query-results/"

def run_emr_job(script_path, job_name):
    print(f"🚀 Launching EMR job: {job_name}...")
    
    response = emr_client.run_job_flow(  # Corrected method here
        Name=job_name,
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}, {"Name": "Hive"}],
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master nodes",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1
                },
                {
                    "Name": "Core nodes",
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1
                }
            ],
            "KeepJobFlowAliveWhenNoSteps": False,
            "Ec2SubnetId": "subnet-9b61e5ba",
            "TerminationProtected": False
        },
        Steps=[
            {
                'Name': job_name,
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': (
                        ['spark-submit', '--deploy-mode', 'cluster', '--master', 'yarn', 'main.py']
                        if script_path.endswith('.zip') else
                        ['spark-submit', '--deploy-mode', 'cluster', '--master', 'yarn', script_path]
                    )
                }
            }
        ],
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        VisibleToAllUsers=True,
        LogUri="s3://glue-hospital-data/emr-logs/"
    )
    
    cluster_id = response['JobFlowId']  # Also updated key here
    print(f"⏳ Waiting for cluster {cluster_id} to complete...")

    waiter = emr_client.get_waiter('cluster_terminated')
    waiter.wait(ClusterId=cluster_id)

    print(f"✅ EMR job {job_name} completed.")

def refresh_athena_table(query_str):
    print("🔁 Refreshing Athena table...")
    response = athena_client.start_query_execution(
        QueryString=query_str,
        QueryExecutionContext={'Database': 'hospital_readmissions'},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    query_id = response['QueryExecutionId']
    print(f"⏳ Waiting for Athena query {query_id} to complete...")

    while True:
        result = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(3)

    if state != 'SUCCEEDED':
        raise Exception(f"Athena query failed with state: {state}")
    print("✅ Athena table refreshed.")

if __name__ == "__main__":
    try:
        # Step 1: Run EMR job for zipped scripts 01 + 02
        run_emr_job(
            script_path="s3://glue-hospital-data/scripts/emr_scripts.zip",
            job_name="Parquet Conversion Job"
        )

        # Step 2: Refresh Athena tables
        refresh_athena_table("MSCK REPAIR TABLE hospital_readmissions.gen_info")
        refresh_athena_table("MSCK REPAIR TABLE hospital_readmissions.readmissions")

        # Step 3: Run EMR job for merge script
        run_emr_job(
            script_path="s3://glue-hospital-data/scripts/run_emr_merge_job.py",
            job_name="Merge Final CSV Job"
        )

        print("🎉 Full pipeline completed successfully.")

    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
