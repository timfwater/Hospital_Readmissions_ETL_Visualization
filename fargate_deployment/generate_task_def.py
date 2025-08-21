import os
import json
import sys

# Validate required environment variables
def get_env(name):
    value = os.environ.get(name)
    if not value:
        print(f"❌ Environment variable '{name}' is missing.")
        sys.exit(1)
    return value

# Load and validate environment variables
AWS_REGION = get_env("AWS_REGION")
ECR_REPO_URI = get_env("ECR_REPO_URI")
TASK_FAMILY = get_env("TASK_FAMILY")
TASK_EXECUTION_ROLE = get_env("TASK_EXECUTION_ROLE")
TASK_ROLE = get_env("TASK_ROLE")
LOG_GROUP = get_env("LOG_GROUP")
LOG_STREAM_PREFIX = get_env("LOG_STREAM_PREFIX")

# 🔒 Explicit log configuration
log_config = {
    "logDriver": "awslogs",
    "options": {
        "awslogs-group": LOG_GROUP,
        "awslogs-region": AWS_REGION,
        "awslogs-stream-prefix": LOG_STREAM_PREFIX
    }
}

# 🧱 Construct the ECS task definition
task_def = {
    "family": TASK_FAMILY,
    "networkMode": "awsvpc",
    "requiresCompatibilities": ["FARGATE"],
    "cpu": "1024",
    "memory": "2048",
    "executionRoleArn": TASK_EXECUTION_ROLE,
    "taskRoleArn": TASK_ROLE,
    "containerDefinitions": [
        {
            "name": TASK_FAMILY,
            "image": f"{ECR_REPO_URI}:latest",
            "essential": True,
            "logConfiguration": log_config
        }
    ]
}

# Write the task definition to JSON
output_path = os.path.join(os.path.dirname(__file__), "final-task-def.json")
with open(output_path, "w") as f:
    json.dump(task_def, f, indent=2)

print(f"📄 Task definition written to: {output_path}")
