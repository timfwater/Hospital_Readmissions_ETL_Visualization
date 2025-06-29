import boto3

emr_client = boto3.client("emr", region_name="us-east-1")

response = emr_client.run_job_flow(
    Name="Hospital Final Merge",
    ReleaseLabel="emr-6.15.0",
    Applications=[{"Name": "Spark"}, {"Name": "Hive"}],
    Instances={
        "InstanceGroups": [
            {
                "Name": "Primary Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core Node",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
        "Ec2SubnetId": "subnet-9b61e5ba",
    },
    Steps=[
        {
            "Name": "Merge from Glue Tables",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode", "cluster",
                    "--master", "yarn",
                    "s3://glue-hospital-data/scripts/03_merge_from_database.py"
                ]
            }
        }
    ],
    VisibleToAllUsers=True,
    JobFlowRole="EMR_EC2_DefaultRole",
    ServiceRole="EMR_DefaultRole",
    LogUri="s3://glue-hospital-data/emr-logs/",
)

print(f"✅ Cluster started: {response['JobFlowId']}")
