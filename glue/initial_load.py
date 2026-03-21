import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from datetime import datetime, timezone

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'postgres_host', 'postgres_port', 'postgres_db',
    'postgres_secret_arn', 'postgres_table',
    'opensearch_endpoint', 'opensearch_index',
    'aws_region', 'checkpoint_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

OS_ENDPOINT = args['opensearch_endpoint'].replace("https://", "")
INDEX_NAME  = args['opensearch_index']
REGION      = args['aws_region']

sm = boto3.client('secretsmanager', region_name=REGION)
secret = json.loads(sm.get_secret_value(SecretId=args['postgres_secret_arn'])['SecretString'])
pg_user = secret['username']
pg_pass = secret['password']
jdbc_url = f"jdbc:postgresql://{args['postgres_host']}:{args['postgres_port']}/{args['postgres_db']}"

bounds = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"(SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM {args['postgres_table']}) AS t") \
    .option("user", pg_user).option("password", pg_pass) \
    .option("driver", "org.postgresql.Driver") \
    .load().collect()[0]

min_id, max_id = bounds['min_id'], bounds['max_id']
print(f"[INFO] ID range: {min_id} -> {max_id}")

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", args['postgres_table']) \
    .option("user", pg_user) \
    .option("password", pg_pass) \
    .option("driver", "org.postgresql.Driver") \
    .option("partitionColumn", "id") \
    .option("lowerBound", str(min_id)) \
    .option("upperBound", str(max_id)) \
    .option("numPartitions", "20") \
    .option("fetchsize", "100") \
    .load()

# Exclude soft-deleted rows if the table uses a deleted_at column
if 'deleted_at' in df.columns:
    df = df.filter(df.deleted_at.isNull())
    print(f"[INFO] Rows to index (after filtering soft-deletes): {df.count()}")
else:
    print(f"[INFO] Total rows to index: {df.count()}")

def get_os_client():
    from opensearchpy import OpenSearch, RequestsHttpConnection
    from requests_aws4auth import AWS4Auth
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    auth = AWS4Auth(creds.access_key, creds.secret_key, REGION, 'aoss', session_token=creds.token)
    return OpenSearch(
        hosts=[{'host': OS_ENDPOINT, 'port': 443}],
        http_auth=auth, use_ssl=True, verify_certs=True,
        connection_class=RequestsHttpConnection, timeout=60
    )

os_client = get_os_client()

if not os_client.indices.exists(index=INDEX_NAME):
    os_client.indices.create(index=INDEX_NAME, body={
        "settings": {
            "index": {
                "refresh_interval": "-1",
                "number_of_replicas": 0,
                "number_of_shards": 5
            }
        },
        "mappings": {
            "properties": {
                "id":         {"type": "integer"},
                "title":      {"type": "text"},
                "body":       {"type": "text", "index_options": "freqs"},
                "status":     {"type": "keyword"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"}
            }
        }
    })
    print(f"[INFO] Created index: {INDEX_NAME}")

def index_partition(partition):
    import json, boto3
    from opensearchpy import OpenSearch, RequestsHttpConnection
    from requests_aws4auth import AWS4Auth

    creds = boto3.Session().get_credentials().get_frozen_credentials()
    auth = AWS4Auth(creds.access_key, creds.secret_key, REGION, 'aoss', session_token=creds.token)
    client = OpenSearch(
        hosts=[{'host': OS_ENDPOINT, 'port': 443}],
        http_auth=auth, use_ssl=True, verify_certs=True,
        connection_class=RequestsHttpConnection, timeout=60
    )

    BATCH_SIZE = 50
    MAX_BYTES  = 5_000_000
    batch, batch_bytes = [], 0

    def flush(b):
        body = []
        for doc in b:
            body.append({"index": {"_index": INDEX_NAME, "_id": str(doc['id'])}})
            body.append(doc)
        resp = client.bulk(body=body)
        if resp.get('errors'):
            failed = [i for i in resp['items'] if i.get('index', {}).get('error')]
            print(f"[WARN] {len(failed)} docs failed in this batch")

    for row in partition:
        doc      = row.asDict()
        doc_size = len(json.dumps(doc, default=str).encode('utf-8'))
        if batch and (len(batch) >= BATCH_SIZE or batch_bytes + doc_size > MAX_BYTES):
            flush(batch)
            batch, batch_bytes = [], 0
        batch.append(doc)
        batch_bytes += doc_size

    if batch:
        flush(batch)

df.foreachPartition(index_partition)

os_client.indices.put_settings(index=INDEX_NAME, body={
    "index": {"refresh_interval": "30s", "number_of_replicas": 1}
})
os_client.indices.forcemerge(index=INDEX_NAME, max_num_segments=5)
print("[INFO] Index settings restored and force merge triggered")

s3 = boto3.client('s3')
watermark = {"last_synced_at": datetime.now(timezone.utc).isoformat()}
s3.put_object(
    Bucket=args['checkpoint_bucket'],
    Key="checkpoints/incremental_watermark.json",
    Body=json.dumps(watermark)
)
print(f"[INFO] Watermark saved: {watermark}")
print("[INFO] Initial load complete.")
job.commit()
