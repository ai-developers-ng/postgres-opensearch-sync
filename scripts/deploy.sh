#!/bin/bash
# Usage: ./scripts/deploy.sh <env> <bucket> <region>
# Example: ./scripts/deploy.sh dev my-assets-bucket-dev us-east-1

set -euo pipefail

ENV=${1:?Usage: deploy.sh <env> <bucket> <region>}
BUCKET=${2:?Usage: deploy.sh <env> <bucket> <region>}
REGION=${3:-us-east-1}
STACK_NAME="opensearch-sync-${ENV}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "==> Downloading AWS RDS global certificate bundle..."
GLOBAL_BUNDLE="$ROOT_DIR/scripts/global-bundle.pem"
if [ ! -f "${GLOBAL_BUNDLE}" ]; then
  curl -sSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o "${GLOBAL_BUNDLE}"
  echo "    global-bundle.pem downloaded."
else
  echo "    global-bundle.pem already present."
fi

echo "==> [1/5] Packaging Lambda dependencies..."
cd "$ROOT_DIR/lambda"
mkdir -p package
cp lambda_function.py package/
cp search_lambda_function.py package/
pip install "psycopg[binary]==3.2.3" opensearch-py==2.4.2 requests-aws4auth==1.2.3 -t package/ -q
cd package && zip -r "$ROOT_DIR/scripts/incremental_sync.zip" . -q && cd "$ROOT_DIR"
echo "    Lambda zip created."

echo "==> Packaging MariaDB sync Lambda..."
cd "$ROOT_DIR/lambda"
mkdir -p mariadb_package
cp mariadb_sync_lambda.py mariadb_package/
cp "${GLOBAL_BUNDLE}" mariadb_package/global-bundle.pem
pip install "psycopg[binary]==3.2.3" PyMySQL==1.1.1 xmltodict==0.13.0 -t mariadb_package/ -q
cd mariadb_package && zip -r "$ROOT_DIR/scripts/mariadb_sync.zip" . -q && cd "$ROOT_DIR"
rm -rf "$ROOT_DIR/lambda/mariadb_package"
echo "    MariaDB sync zip created."

echo "==> [2/5] Packaging Glue Python dependencies..."
mkdir -p "$ROOT_DIR/glue_package"
pip install opensearch-py==2.4.2 requests-aws4auth==1.2.3 -t "$ROOT_DIR/glue_package/" -q
cd "$ROOT_DIR/glue_package" && zip -r "$ROOT_DIR/scripts/dependencies.zip" . -q && cd "$ROOT_DIR"
rm -rf "$ROOT_DIR/glue_package"
echo "    Glue dependencies zip created."

echo "==> [3/5] Uploading assets to S3..."
aws s3 cp "$ROOT_DIR/glue/initial_load.py"                     "s3://${BUCKET}/scripts/initial_load.py" --region "${REGION}"
aws s3 cp "$ROOT_DIR/glue/xml_to_postgres_initial_load.py"     "s3://${BUCKET}/scripts/xml_to_postgres_initial_load.py" --region "${REGION}"
aws s3 cp "$ROOT_DIR/scripts/incremental_sync.zip"             "s3://${BUCKET}/scripts/incremental_sync.zip" --region "${REGION}"
aws s3 cp "$ROOT_DIR/scripts/mariadb_sync.zip"                 "s3://${BUCKET}/scripts/mariadb_sync.zip" --region "${REGION}"
aws s3 cp "$ROOT_DIR/scripts/dependencies.zip"                 "s3://${BUCKET}/jars/dependencies.zip" --region "${REGION}"
aws s3 cp "${GLOBAL_BUNDLE}"                                   "s3://${BUCKET}/scripts/global-bundle.pem" --region "${REGION}"
echo "    Assets uploaded."

if [ -f "$ROOT_DIR/scripts/postgresql-42.7.3.jar" ]; then
  aws s3 cp "$ROOT_DIR/scripts/postgresql-42.7.3.jar" "s3://${BUCKET}/jars/postgresql-42.7.3.jar" --region "${REGION}"
  echo "    JDBC driver uploaded."
else
  echo "    [WARN] postgresql-42.7.3.jar not found in scripts/ — upload manually if not already in S3"
fi

echo "==> [4/5] Deploying CloudFormation stack: ${STACK_NAME}..."
aws cloudformation deploy \
  --template-file "$ROOT_DIR/cloudformation/template.yaml" \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "${REGION}" \
  --parameter-overrides \
    Environment="${ENV}" \
    AssetsBucketName="${BUCKET}" \
  --no-fail-on-empty-changeset

echo "==> [5/5] Stack outputs:"
aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
  --output table

echo ""
echo "==> Done! Next steps:"
echo "    1. Run: ./scripts/update_opensearch_access.sh ${ENV} <collection> <policy-name> ${REGION}"
echo "    2. Run: aws glue start-job-run --job-name postgres-to-opensearch-initial-load-${ENV} --region ${REGION}"
