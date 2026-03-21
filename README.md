# postgres-opensearch-sync

Syncs data from PostgreSQL to Amazon OpenSearch Serverless.
- **Glue** handles the one-time 35GB initial load using partitioned JDBC reads and foreachPartition bulk indexing.
- **Lambda** runs every minute via EventBridge to pick up new or updated rows using a watermark in S3.

## Structure

```
postgres-opensearch-sync/
├── cloudformation/template.yaml        # Full CFN stack
├── glue/initial_load.py                # Glue PySpark initial load
├── lambda/lambda_function.py           # Lambda incremental sync
├── scripts/
│   ├── deploy.sh                       # Package + deploy
│   ├── update_opensearch_access.sh     # Update AOSS data access policy
│   └── postgresql-42.7.3.jar           # Place JDBC driver here
└── .gitignore
```

## Deploy

```bash
chmod +x scripts/deploy.sh scripts/update_opensearch_access.sh

./scripts/deploy.sh dev  my-bucket-dev  us-east-1
./scripts/deploy.sh stg  my-bucket-stg  us-east-1
./scripts/deploy.sh prd  my-bucket-prd  us-east-1
```

## Post-Deploy

```bash
./scripts/update_opensearch_access.sh prd my-collection glue-access us-east-1
aws glue start-job-run --job-name postgres-to-opensearch-initial-load-prd
```

## Environments

| Setting              | dev | stg | prd |
|----------------------|-----|-----|-----|
| Glue timeout (min)   | 60  | 120 | 240 |
| Glue workers         | 2   | 5   | 10  |
| Log retention (days) | 3   | 7   | 30  |
