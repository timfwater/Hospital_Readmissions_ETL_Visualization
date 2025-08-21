#!/bin/bash
set -e

# Load and export environment variables
source "$(dirname "$0")/config.env"
export AWS_ACCOUNT_ID AWS_REGION ECS_CLUSTER_NAME TASK_FAMILY TASK_EXECUTION_ROLE TASK_ROLE \
       FARGATE_SUBNET_IDS FARGATE_SECURITY_GROUP_IDS ECR_REPO_NAME LOG_GROUP LOG_STREAM_PREFIX \
       SCRIPT_BUCKET ATHENA_OUTPUT_PREFIX

export ECR_REPO_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}"

echo "🔧 Checking for ECS cluster..."
if ! aws ecs describe-clusters --clusters "$ECS_CLUSTER_NAME" --region "$AWS_REGION" | grep -q "\"clusterName\": \"$ECS_CLUSTER_NAME\""; then
  aws ecs create-cluster --cluster-name "$ECS_CLUSTER_NAME" --region "$AWS_REGION" > /dev/null
  echo "✅ Created ECS cluster: $ECS_CLUSTER_NAME"
else
  echo "ℹ️ ECS cluster already exists: $ECS_CLUSTER_NAME"
fi

echo "🪵 Checking for CloudWatch log group..."
if ! aws logs describe-log-groups --log-group-name-prefix "$LOG_GROUP" --region "$AWS_REGION" | grep -q "\"logGroupName\": \"$LOG_GROUP\""; then
  aws logs create-log-group --log-group-name "$LOG_GROUP" --region "$AWS_REGION"
  echo "✅ Created log group: $LOG_GROUP"
else
  echo "ℹ️ Log group already exists: $LOG_GROUP"
fi

echo "🔐 Ensuring ECS task role has EMR permissions..."
POLICY_NAME="AllowEMRAccessForETL"
POLICY_DOCUMENT=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "elasticmapreduce:RunJobFlow",
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:TerminateJobFlows"
      ],
      "Resource": "*"
    }
  ]
}
EOF
)

aws iam put-role-policy \
  --role-name "$(basename $TASK_ROLE)" \
  --policy-name "$POLICY_NAME" \
  --policy-document "$POLICY_DOCUMENT" \
  --region "$AWS_REGION"

echo "✅ Inline EMR policy applied to task role: $TASK_ROLE"

echo "📦 Building and pushing Docker image..."
if ! aws ecr describe-repositories --repository-names "$ECR_REPO_NAME" --region "$AWS_REGION" > /dev/null 2>&1; then
  aws ecr create-repository --repository-name "$ECR_REPO_NAME" --region "$AWS_REGION"
  echo "✅ Created ECR repository: $ECR_REPO_NAME"
else
  echo "ℹ️ ECR repository already exists: $ECR_REPO_NAME"
fi

aws ecr get-login-password --region "$AWS_REGION" | \
docker login --username AWS --password-stdin "$ECR_REPO_URI"

docker build -t "$ECR_REPO_NAME" ./
docker tag "$ECR_REPO_NAME:latest" "$ECR_REPO_URI:latest"
docker push "$ECR_REPO_URI:latest"

echo "📝 Generating and registering task definition..."
python3 "$(dirname "$0")/generate_task_def.py"

# Validate and register the task definition
if python3 -m json.tool "$(dirname "$0")/final-task-def.json" > /dev/null 2>&1; then
  aws ecs register-task-definition \
    --cli-input-json "file://$(dirname "$0")/final-task-def.json" \
    --region "$AWS_REGION"
  echo "✅ Task definition registered."
else
  echo "❌ Invalid task definition JSON. Aborting."
  exit 1
fi

echo "🚀 Deployment to Fargate complete."

echo "🚀 Running ECS task on Fargate..."
aws ecs run-task \
  --cluster "$ECS_CLUSTER_NAME" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[${FARGATE_SUBNET_IDS}],securityGroups=[${FARGATE_SECURITY_GROUP_IDS}],assignPublicIp=\"ENABLED\"}" \
  --task-definition "$TASK_FAMILY" \
  --region "$AWS_REGION"
