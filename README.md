# postgres-opensearch-sync

Syncs data from PostgreSQL to Amazon OpenSearch Serverless.
- **Glue** handles the one-time 35GB initial load using partitioned JDBC reads and foreachPartition bulk indexing.
- **Lambda** runs every minute via EventBridge to pick up new or updated rows using a watermark in S3.
- **Search Lambda** provides query-time search against OpenSearch, exposed via an API Gateway REST API secured with an API key.

## Structure

```
postgres-opensearch-sync/
├── cloudformation/template.yaml        # Full CFN stack
├── glue/initial_load.py                # Glue PySpark initial load
├── lambda/lambda_function.py           # Lambda incremental sync
├── lambda/search_lambda_function.py    # Lambda OpenSearch search handler
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

## Search API

The search Lambda is exposed via two API Gateway endpoints serving different use cases:

| | REST API (v1) | HTTP API (v2) |
|---|---|---|
| Auth | API key (`x-api-key` header) | None (open) |
| Use case | Appian / managed clients | Internal tooling, lightweight clients |
| CFN output | `SearchApiEndpoint` | `SearchHttpApiEndpoint` |
| Throttling | 20 rps / burst 50 / 100k/month | 20 rps / burst 50 |

Both expose `GET /search` and accept the same query parameters.

### Query parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `q` | Yes | — | Search query string |
| `size` | No | `10` | Number of results (1–100) |

### Response body

```json
{
  "query": "invoice",
  "total_hits": 42,
  "results": [
    {
      "id": "abc123",
      "score": 4.52,
      "matching_percentage": 100.0,
      "source": { "...": "document fields" }
    }
  ]
}
```

### REST API — getting the API key value

After deploying the stack, retrieve the API key value:

```bash
KEY_ID=$(aws cloudformation describe-stacks \
  --stack-name <your-stack-name> \
  --query "Stacks[0].Outputs[?OutputKey=='SearchApiKeyId'].OutputValue" \
  --output text)

aws apigateway get-api-key --api-key "$KEY_ID" --include-value --query value --output text
```

### REST API — calling with curl

```bash
curl -G "https://<api-id>.execute-api.<region>.amazonaws.com/<env>/search" \
  --header "x-api-key: <your-api-key>" \
  --data-urlencode "q=invoice" \
  --data-urlencode "size=10"
```

### HTTP API — calling with curl

```bash
curl -G "https://<http-api-id>.execute-api.<region>.amazonaws.com/<env>/search" \
  --data-urlencode "q=invoice" \
  --data-urlencode "size=10"
```

### Configuring Appian (HTTP Connected System)

Use the **REST API** endpoint so the API key is enforced.

1. Create a new **HTTP Connected System** in Appian.
2. Set **Base URL** to the `SearchApiEndpoint` stack output value.
3. Add a static HTTP header: `x-api-key` → paste the key value retrieved above.
4. In your Integration, use `GET` method and append `?q={searchTerm}` as a query parameter.

### Testing with Postman

#### REST API (with API key)

1. Create a new request in Postman.
2. Set method to **GET** and URL to:
   ```
   https://<api-id>.execute-api.<region>.amazonaws.com/<env>/search
   ```
3. Go to the **Headers** tab and add:
   | Key | Value |
   |-----|-------|
   | `x-api-key` | `<your-api-key>` |
4. Go to the **Params** tab and add:
   | Key | Value |
   |-----|-------|
   | `q` | `invoice` |
   | `size` | `10` |
5. Click **Send**. You should get a `200` response with results.

   > Without the `x-api-key` header you will receive `403 Forbidden`.

#### HTTP API (no key required)

1. Create a new request in Postman.
2. Set method to **GET** and URL to:
   ```
   https://<http-api-id>.execute-api.<region>.amazonaws.com/<env>/search
   ```
3. Go to the **Params** tab and add:
   | Key | Value |
   |-----|-------|
   | `q` | `invoice` |
   | `size` | `10` |
4. Click **Send**. No headers needed.

### Testing directly from the Lambda Console

You can invoke the search Lambda without going through API Gateway using the AWS Console **Test** tab.

1. Open the `opensearch-search-<env>` Lambda function in the AWS Console.
2. Click the **Test** tab.
3. Select **Create new event** and give it a name (e.g. `search-invoice`).
4. Paste one of the event payloads below and click **Test**.

**Simplest — top-level keys (no API Gateway wrapper):**
```json
{
  "q": "invoice",
  "size": 10
}
```

**Via `queryStringParameters` (mimics API Gateway GET request):**
```json
{
  "queryStringParameters": {
    "q": "invoice",
    "size": "10"
  }
}
```

**Full API Gateway proxy format:**
```json
{
  "httpMethod": "GET",
  "queryStringParameters": {
    "q": "invoice",
    "size": "10"
  },
  "body": null,
  "isBase64Encoded": false
}
```

> The handler reads `q` from `queryStringParameters`, `body`, or the top-level event — all three formats above work.

#### Using Postman environments (recommended)

Create a Postman environment with these variables to avoid repeating values across requests:

| Variable | Example value |
|----------|---------------|
| `rest_api_url` | `https://<api-id>.execute-api.<region>.amazonaws.com/<env>/search` |
| `http_api_url` | `https://<http-api-id>.execute-api.<region>.amazonaws.com/<env>/search` |
| `api_key` | `<your-api-key>` |

Then in your requests use `{{rest_api_url}}`, `{{http_api_url}}`, and `{{api_key}}` instead of hardcoded values.

## Environments

| Setting              | dev | stg | prd |
|----------------------|-----|-----|-----|
| Glue timeout (min)   | 60  | 120 | 240 |
| Glue workers         | 2   | 5   | 10  |
| Log retention (days) | 3   | 7   | 30  |
