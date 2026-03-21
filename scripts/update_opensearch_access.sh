#!/bin/bash
# Usage: ./scripts/update_opensearch_access.sh <env> <collection_name> <policy_name> <region>
# Example: ./scripts/update_opensearch_access.sh prd my-collection glue-access us-east-1

set -euo pipefail

ENV=${1:?Usage: update_opensearch_access.sh <env> <collection_name> <policy_name> <region>}
COLLECTION=${2:?Usage: update_opensearch_access.sh <env> <collection_name> <policy_name> <region>}
POLICY_NAME=${3:?Usage: update_opensearch_access.sh <env> <collection_name> <policy_name> <region>}
REGION=${4:-us-east-1}
STACK_NAME="opensearch-sync-${ENV}"

echo "==> Fetching role ARNs from stack: ${STACK_NAME}..."
GLUE_ROLE=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query 'Stacks[0].Outputs[?OutputKey==`GlueRoleArn`].OutputValue' --output text)

LAMBDA_ROLE=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query 'Stacks[0].Outputs[?OutputKey==`LambdaRoleArn`].OutputValue' --output text)

echo "    Glue Role:   ${GLUE_ROLE}"
echo "    Lambda Role: ${LAMBDA_ROLE}"

POLICY=$(cat <<POLICY
[{
  "Rules": [
    {
      "Resource": ["collection/${COLLECTION}"],
      "Permission": ["aoss:CreateCollectionItems","aoss:UpdateCollectionItems","aoss:DescribeCollectionItems"],
      "ResourceType": "collection"
    },
    {
      "Resource": ["index/${COLLECTION}/*"],
      "Permission": ["aoss:CreateIndex","aoss:UpdateIndex","aoss:DescribeIndex","aoss:WriteDocument","aoss:ReadDocument"],
      "ResourceType": "index"
    }
  ],
  "Principal": ["${GLUE_ROLE}","${LAMBDA_ROLE}"]
}]
POLICY
)

echo "==> Updating OpenSearch Serverless data access policy: ${POLICY_NAME}..."
if aws opensearchserverless get-access-policy --name "${POLICY_NAME}" --type data --region "${REGION}" &>/dev/null; then
  aws opensearchserverless update-access-policy --name "${POLICY_NAME}" --type data --policy "${POLICY}" --region "${REGION}"
  echo "    Policy updated."
else
  aws opensearchserverless create-access-policy --name "${POLICY_NAME}" --type data --policy "${POLICY}" --region "${REGION}"
  echo "    Policy created."
fi
echo "==> Done! Both roles now have access to collection: ${COLLECTION}"
