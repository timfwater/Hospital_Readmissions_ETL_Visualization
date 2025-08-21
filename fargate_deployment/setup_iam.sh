#!/bin/bash
set -e

# Load env variables
source "$(dirname "$0")/config.env"

echo "🔧 Setting up IAM roles and policies..."

# Use hardcoded role names from config (not full ARNs)
EXEC_ROLE_NAME="HospitalETLECSExecutionRole"
TASK_ROLE_NAME="HospitalETLECSTaskRole"

# Create ECS Execution Role (trust policy)
aws iam create-role --role-name "$EXEC_ROLE_NAME" \
  --assume-role-policy-document file://$(dirname "$0")/ecs-trust-policy.json \
  || echo "ℹ️ Execution role already exists: $EXEC_ROLE_NAME"

# Attach default ECS Execution policy
aws iam attach-role-policy \
  --role-name "$EXEC_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy \
  || echo "ℹ️ Policy already attached to $EXEC_ROLE_NAME"

# Create ECS Task Role (trust policy)
aws iam create-role --role-name "$TASK_ROLE_NAME" \
  --assume-role-policy-document file://$(dirname "$0")/ecs-trust-policy.json \
  || echo "ℹ️ Task role already exists: $TASK_ROLE_NAME"

# Attach inline S3 and EMR/Athena access policy
aws iam put-role-policy \
  --role-name "$TASK_ROLE_NAME" \
  --policy-name S3AndECSAccessPolicy \
  --policy-document file://$(dirname "$0")/s3-access-policy.json \
  || echo "ℹ️ Policy already exists or updated."

# Attach optional managed policies
aws iam attach-role-policy \
  --role-name "$TASK_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess

aws iam attach-role-policy \
  --role-name "$TASK_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
  --role-name "$TASK_ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess

echo "✅ IAM roles and policies configured successfully."
