# postgres-opensearch-sync

Syncs data from two sources into Amazon OpenSearch Serverless, with a unified search Lambda on top.

- **PostgreSQL → OpenSearch `documents` index**: Glue PySpark initial load + Lambda incremental sync (watermark-based, every minute).
- **MariaDB XML → PostgreSQL → OpenSearch `records` index**: Glue Python Shell initial load parses TKRSUMMARY XML → JSON into PostgreSQL `tkrsummary`, then a second Lambda syncs PostgreSQL → OpenSearch `records` index.
- **Search Lambda**: query-time search across either index, exposed via REST API (API key) and HTTP API (open).

## Architecture

```
MariaDB TKRSUMMARY          PostgreSQL
  (XML: SUMMARYCLOB)  ───►  tkrsummary       ───► OpenSearch records index
                             (jsonb)
PostgreSQL documents  ───────────────────────► OpenSearch documents index

                                    OpenSearch
                                        │
                               search_lambda_function
                                        │
                         ┌─────────────┴─────────────┐
                  REST API (v1)               HTTP API (v2)
                  (API key required)          (open)
```

## Repository Structure

```
postgres-opensearch-sync/
├── cloudformation/
│   └── template.yaml                    # Full CloudFormation stack
├── glue/
│   ├── initial_load.py                              # PySpark: PostgreSQL → OpenSearch initial load
│   ├── xml_to_postgres_initial_load.py              # Python Shell: MariaDB XML → PostgreSQL initial load
│   └── tkrsummary_to_opensearch_initial_load.py     # Python Shell: PostgreSQL tkrsummary → OpenSearch records initial load
├── lambda/
│   ├── lambda_function.py               # Incremental sync: PostgreSQL → OpenSearch
│   ├── mariadb_sync_lambda.py           # Incremental sync: MariaDB → PostgreSQL (30s)
│   └── search_lambda_function.py        # Search handler (dual-mode: structured + unstructured)
└── scripts/
    ├── deploy.sh
    └── update_opensearch_access.sh
```

## Deploy

```bash
chmod +x scripts/deploy.sh scripts/update_opensearch_access.sh

./scripts/deploy.sh dev  my-bucket-dev  us-east-1
./scripts/deploy.sh stg  my-bucket-stg  us-east-1
./scripts/deploy.sh prd  my-bucket-prd  us-east-1
```

### CloudFormation Parameters

| Parameter | Description |
|-----------|-------------|
| `PostgresHost` | RDS PostgreSQL endpoint |
| `PostgresPort` | Default `5445` |
| `PostgresDb` | Database name |
| `PostgresTable` | Source table e.g. `public.documents` |
| `PostgresSecretName` | Secrets Manager secret name for PostgreSQL |
| `PostgresSecretArn` | Full ARN of the PostgreSQL secret |
| `MariadbSecretName` | Secrets Manager secret name for MariaDB |
| `MariadbSecretArn` | Full ARN of the MariaDB secret |
| `MariadbPort` | Default `3306` |
| `OpenSearchEndpoint` | Collection endpoint (`https://xxx.region.aoss.amazonaws.com`) |
| `OpenSearchCollectionId` | Collection ID (for IAM resource ARNs) |
| `OpenSearchIndex` | Target index for `documents` pipeline |
| `RecordsOpenSearchIndex` | Target index for `records` pipeline (default `records`) |
| `AssetsBucketName` | S3 bucket for scripts, JARs, and S3 checkpoints |
| `NotificationEmail` | Email for SNS failure alerts |

Secrets Manager secrets must have keys: `username`, `password`, `host` (MariaDB only), `dbname` (MariaDB only).

## Post-Deploy

```bash
# Update OpenSearch data access policy to allow Glue + Lambda IAM roles
./scripts/update_opensearch_access.sh prd my-collection glue-access us-east-1

# Run initial loads IN ORDER (wait for each to complete before starting the next)
# Step 1: Load MariaDB XML → PostgreSQL tkrsummary (also seeds mariadb_sync checkpoint)
aws glue start-job-run --job-name xml-to-postgres-initial-load-prd

# Step 2: Bulk index tkrsummary → OpenSearch records (also seeds tkrsummary_watermark)
aws glue start-job-run --job-name tkrsummary-to-opensearch-initial-load-prd

# Step 3: Bulk index PostgreSQL documents → OpenSearch documents
aws glue start-job-run --job-name postgres-to-opensearch-initial-load-prd

# Step 4: Re-enable incremental sync (was disabled to avoid conflict with initial load)
aws events enable-rule --name mariadb-xml-sync-schedule-prd
```

## Data Pipelines

### 1. PostgreSQL → OpenSearch `documents`

| Component | Detail |
|-----------|--------|
| Initial load | Glue PySpark (`glue/initial_load.py`), partitioned JDBC read, foreachPartition bulk indexing |
| Incremental sync | Lambda `lambda_function.handler`, EventBridge `rate(1 minute)`, watermark in S3 at `checkpoints/checkpoint.json` |
| CFN resources | `GlueInitialLoadJob`, `IncrementalSyncLambda`, `SyncScheduleRule` |

### 2. MariaDB → PostgreSQL `tkrsummary`

| Component | Detail |
|-----------|--------|
| Source | MariaDB `TKRSUMMARY` table — columns `TKRID`, `CREATE_DATE`, `SUMMARYCLOB` (XML) |
| Initial load | Glue Python Shell (`glue/xml_to_postgres_initial_load.py`) — keyset pagination on `(CREATE_DATE, TKRID)`, `xmltodict` → `json.dumps` → PostgreSQL `jsonb` |
| Incremental sync | Lambda `mariadb_sync_lambda.handler`, 90s timeout, double-cycle per 1-min trigger (run → `sleep(28)` → run), checkpoint at `checkpoints/mariadb_sync_checkpoint.json` |
| Upsert key | `ON CONFLICT (tkrid) DO UPDATE` — `id` column is auto-sequence, never inserted |
| CFN resources | `GlueXmlToPostgresJob`, `MariadbSyncLambda`, `MariadbSyncScheduleRule` |

> The double-cycle pattern gives ~30s effective polling despite EventBridge's 1-minute minimum.

### 3. PostgreSQL `tkrsummary` → OpenSearch `records`

| Component | Detail |
|-----------|--------|
| Initial load | Glue Python Shell (`glue/tkrsummary_to_opensearch_initial_load.py`) — keyset pagination on `tkrid`, bulk indexes into `records` index, seeds `checkpoints/tkrsummary_watermark.json` on completion |
| Incremental sync | Lambda `lambda_function.handler` (same code as pipeline 1), pointed at `tkrsummary` table and `records` index, watermark at `checkpoints/tkrsummary_watermark.json` |
| CFN resources | `GlueTkrsummaryToOSJob`, `RecordsSyncLambda`, `RecordsSyncScheduleRule` |

> **Run order for initial setup:** `xml-to-postgres` → `tkrsummary-to-opensearch` → `postgres-to-opensearch` → re-enable `mariadb-xml-sync-schedule` rule.

## Search API

### Endpoints

| | REST API (v1) | HTTP API (v2) |
|---|---|---|
| Auth | API key (`x-api-key` header) | None (open) |
| Use case | Appian / managed clients | Internal tooling |
| CFN output | `SearchApiEndpoint` | `SearchHttpApiEndpoint` |
| Throttling | 20 rps / burst 50 / 100k req/month | 20 rps / burst 50 |

Both expose `POST /search` (body) and `GET /search` (query params).

### Dual-Mode Queries

The search Lambda automatically selects a query strategy based on the presence of `must_match` or `date_filters`.

#### Unstructured mode — keyword search across all fields

Used when only `q` is provided. Runs a `query_string` with `AND` operator across all fields.

```json
{
  "q": "cardiac risk assessment",
  "size": 10
}
```

Also works with `index` to target `documents` or `records`:
```json
{
  "index": "records",
  "q": "cardiac risk"
}
```

#### Structured mode — exact matches + date filters

Used when `must_match` or `date_filters` are present. Runs a `bool` query:
- `must` → `query_string` for keywords + `match_phrase` per `must_match` field (100% phrase match)
- `filter` → `range` per date field (doesn't affect relevance score)

```json
{
  "index": "records",
  "q": "cardiac risk",
  "must_match": {
    "category": "clinical",
    "status": "approved"
  },
  "date_filters": {
    "createddt": { "gte": "2024-01-01", "lte": "2024-12-31" }
  },
  "size": 20
}
```

Exact match only (no keyword):
```json
{
  "index": "records",
  "must_match": { "tkrid": "TKR-00142" }
}
```

Date range only:
```json
{
  "index": "records",
  "date_filters": {
    "createddt": { "gte": "2025-01-01" },
    "updateddt": { "lte": "2026-03-01" }
  }
}
```

### Request Parameters

| Parameter | Source | Default | Description |
|-----------|--------|---------|-------------|
| `q` | body or `?q=` | — | Keyword search string. Supports `OR`, `NOT`, wildcards (`*`, `?`), proximity (`~n`) |
| `index` | body or `?index=` | `OPENSEARCH_INDEX` env var | `documents` or `records` |
| `size` | body or `?size=` | `10` | Results to return (1–100) |
| `must_match` | body only | — | `{field: value}` — each entry becomes a `match_phrase` (100% match) |
| `date_filters` | body only | — | `{field: {gte, lte, gt, lt}}` — applied as `range` filter |

### Response

```json
{
  "query": "cardiac risk",
  "must_match": { "category": "clinical" },
  "date_filters": { "createddt": { "gte": "2024-01-01" } },
  "index": "records",
  "total_hits": 14,
  "results": [
    {
      "id": "TKR-00142",
      "score": 4.52,
      "matching_percentage": 100.0,
      "source": { "...": "document fields" }
    }
  ]
}
```

`must_match` and `date_filters` are `null` in the response when not used.

### Retrieve the REST API Key

```bash
KEY_ID=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query "Stacks[0].Outputs[?OutputKey=='SearchApiKeyId'].OutputValue" \
  --output text)

aws apigateway get-api-key --api-key "$KEY_ID" --include-value --query value --output text
```

### curl Examples

**REST API (requires API key):**
```bash
# Unstructured GET
curl -G "https://<api-id>.execute-api.<region>.amazonaws.com/<env>/search" \
  -H "x-api-key: <key>" \
  --data-urlencode "q=cardiac risk" \
  --data-urlencode "size=10"

# Structured POST
curl -X POST "https://<api-id>.execute-api.<region>.amazonaws.com/<env>/search" \
  -H "x-api-key: <key>" \
  -H "Content-Type: application/json" \
  -d '{"index":"records","q":"cardiac","must_match":{"status":"approved"},"date_filters":{"createddt":{"gte":"2024-01-01"}}}'
```

**HTTP API (no key):**
```bash
curl -G "https://<http-api-id>.execute-api.<region>.amazonaws.com/<env>/search" \
  --data-urlencode "q=cardiac risk"
```

### Configuring Appian

Use the **REST API** endpoint so the API key is enforced.

1. Create a new **HTTP Connected System** in Appian.
2. Set **Base URL** to the `SearchApiEndpoint` stack output.
3. Add static header: `x-api-key` → API key value from above.
4. In your Integration, use `POST` and set the JSON body with your search parameters.

### Lambda Console Test Payloads

**Unstructured (top-level keys):**
```json
{
  "q": "cardiac risk",
  "size": 10
}
```

**Structured (via body):**
```json
{
  "body": "{\"index\":\"records\",\"q\":\"cardiac\",\"must_match\":{\"status\":\"approved\"},\"date_filters\":{\"createddt\":{\"gte\":\"2024-01-01\"}}}",
  "isBase64Encoded": false
}
```

**Via `queryStringParameters` (mimics API Gateway GET):**
```json
{
  "queryStringParameters": {
    "q": "cardiac risk",
    "index": "records",
    "size": "10"
  }
}
```

## Environments

| Setting | dev | stg | prd |
|---------|-----|-----|-----|
| Glue timeout (min) | 60 | 120 | 240 |
| Glue workers | 2 | 5 | 10 |
| Lambda timeout (s) | 58 | 58 | 58 |
| MariaDB sync timeout (s) | 90 | 90 | 90 |
| Log retention (days) | 3 | 7 | 30 |

## CloudFormation Outputs

| Output | Description |
|--------|-------------|
| `GlueJobName` | PySpark initial load job |
| `XmlToPostgresGlueJobName` | Python Shell XML initial load job |
| `LambdaFunctionName` | documents incremental sync Lambda |
| `MariadbSyncLambdaName` | MariaDB → PostgreSQL sync Lambda |
| `RecordsSyncLambdaName` | PostgreSQL → OpenSearch records sync Lambda |
| `SearchLambdaFunctionName` | Search Lambda |
| `SearchApiEndpoint` | REST API URL (requires `x-api-key` header) |
| `SearchApiKeyId` | API key ID — retrieve value with `aws apigateway get-api-key` |
| `SearchHttpApiEndpoint` | HTTP API URL (no auth) |
| `GlueRoleArn` | Add to OpenSearch data access policy |
| `LambdaRoleArn` | Add to OpenSearch data access policy |
| `AlertTopicArn` | SNS topic for failure notifications |
