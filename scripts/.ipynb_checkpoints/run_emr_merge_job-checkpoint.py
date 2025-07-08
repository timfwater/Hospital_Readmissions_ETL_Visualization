import boto3
import time

REGION = "us-east-1"
S3_SCRIPT_BUCKET = "s3://glue-hospital-data/scripts/"
SUBNET_ID = "subnet-9b61e5ba"
INSTANCE_TYPE = "m5.xlarge"

emr_client = boto3.client("emr", region_name=REGION)

def wait_for_cluster(cluster_id):
    print(f"🔄 Waiting for EMR cluster {cluster_id} to finish...")
    while True:
        response = emr_client.describe_cluster(ClusterId=cluster_id)
        state = response["Cluster"]["Status"]["State"]
        print(f"  Current status: {state}")
        if state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            break
        time.sleep(30)
    print(f"✅ Cluster {cluster_id} finished.")

def launch_emr_merge_cluster():
    print("🚀 Launching EMR cluster for final merge...")
    response = emr_client.run_job_flow(
        Name="Merge Final CSV Job",
        ReleaseLabel="emr-6.15.0",
        Applications=[{"Name": "Spark"}],
        Instances={
            "InstanceGroups": [
                {
                    "InstanceRole": "MASTER",
                    "InstanceType": INSTANCE_TYPE,
                    "InstanceCount": 1,
                },
                {
                    "InstanceRole": "CORE",
                    "InstanceType": INSTANCE_TYPE,
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
                "Name": "Step 3: Merge and Clean",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        f"{S3_SCRIPT_BUCKET}03_merge_from_database.py"
                    ],
                },
            }
        ],
        AutoTerminationPolicy={"IdleTimeout": 600},
    )
    cluster_id = response["JobFlowId"]
    print(f"🆔 EMR Cluster ID: {cluster_id}")
    return cluster_id

# --- Run everything ---
if __name__ == "__main__":
    cluster_id = launch_emr_merge_cluster()
    wait_for_cluster(cluster_id)
