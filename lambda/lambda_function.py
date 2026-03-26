import json
import os
import boto3
import psycopg
from datetime import datetime, timezone, timedelta
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION            = os.environ['AWS_REGION']
SECRET_ARN        = os.environ['POSTGRES_SECRET_ARN']
POSTGRES_HOST     = os.environ['POSTGRES_HOST']
POSTGRES_PORT     = int(os.environ.get('POSTGRES_PORT', 5445))
POSTGRES_DB       = os.environ['POSTGRES_DB']
POSTGRES_TABLE    = os.environ['POSTGRES_TABLE']
OS_ENDPOINT       = os.environ['OPENSEARCH_ENDPOINT'].replace("https://", "")
OS_INDEX          = os.environ['OPENSEARCH_INDEX']
CHECKPOINT_BUCKET = os.environ['CHECKPOINT_BUCKET']
CHECKPOINT_KEY    = os.environ.get('CHECKPOINT_KEY', 'checkpoints/incremental_watermark.json')
BATCH_SIZE        = int(os.environ.get('BATCH_SIZE', 50))
MAX_BYTES         = 5_000_000
ALERT_TOPIC_ARN   = os.environ.get('ALERT_TOPIC_ARN')

s3_client  = boto3.client('s3')
sm_client  = boto3.client('secretsmanager', region_name=REGION)
sns_client = boto3.client('sns', region_name=REGION) if ALERT_TOPIC_ARN else None


def get_pg_credentials():
    secret = sm_client.get_secret_value(SecretId=SECRET_ARN)
    return json.loads(secret['SecretString'])


def get_pg_connection():
    creds = get_pg_credentials()
    return psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=creds['username'], password=creds['password'],
        connect_timeout=10, options='-c statement_timeout=55000'
    )


def get_os_client():
    credentials = boto3.Session().get_credentials().get_frozen_credentials()
    awsauth = AWS4Auth(
        credentials.access_key, credentials.secret_key,
        REGION, 'aoss', session_token=credentials.token
    )
    return OpenSearch(
        hosts=[{'host': OS_ENDPOINT, 'port': 443}],
        http_auth=awsauth, use_ssl=True, verify_certs=True,
        connection_class=RequestsHttpConnection, timeout=30
    )


def load_watermark():
    try:
        obj = s3_client.get_object(Bucket=CHECKPOINT_BUCKET, Key=CHECKPOINT_KEY)
        return json.loads(obj['Body'].read())['last_synced_at']
    except s3_client.exceptions.NoSuchKey:
        fallback = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        print(f"[INFO] No watermark found, using fallback: {fallback}")
        return fallback


def save_watermark(timestamp: str):
    s3_client.put_object(
        Bucket=CHECKPOINT_BUCKET, Key=CHECKPOINT_KEY,
        Body=json.dumps({"last_synced_at": timestamp})
    )


def alert(subject: str, message: str):
    if sns_client and ALERT_TOPIC_ARN:
        try:
            sns_client.publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject, Message=message)
        except Exception as e:
            print(f"[WARN] Failed to publish SNS alert: {e}")


def bulk_upsert(os_client, docs: list) -> dict:
    body = []
    for doc in docs:
        body.append({"index": {"_index": OS_INDEX, "_id": str(doc['id'])}})
        body.append(doc)
    response = os_client.bulk(body=body)

    failed_docs = []
    if response.get('errors'):
        id_to_doc = {str(doc['id']): doc for doc in docs}
        for item in response['items']:
            idx = item.get('index', {})
            if idx.get('error'):
                print(f"[WARN] Failed doc {idx.get('_id')}: {idx.get('error', {}).get('reason')}")
                if idx.get('_id') in id_to_doc:
                    failed_docs.append(id_to_doc[idx['_id']])

    # Retry failed docs once
    still_failed = 0
    if failed_docs:
        retry_body = []
        for doc in failed_docs:
            retry_body.append({"index": {"_index": OS_INDEX, "_id": str(doc['id'])}})
            retry_body.append(doc)
        retry_response = os_client.bulk(body=retry_body)
        if retry_response.get('errors'):
            still_failed = sum(
                1 for item in retry_response['items'] if item.get('index', {}).get('error')
            )
            print(f"[ERROR] {still_failed} docs still failed after retry")
        else:
            print(f"[INFO] Retry succeeded for {len(failed_docs)} docs")

    return {"indexed": len(docs) - still_failed, "failed": still_failed}


def bulk_delete(os_client, ids: list) -> dict:
    body = []
    for doc_id in ids:
        body.append({"delete": {"_index": OS_INDEX, "_id": str(doc_id)}})
    response = os_client.bulk(body=body)
    failed = 0
    if response.get('errors'):
        # not_found is expected (already deleted or never indexed); don't count as failure
        failed = sum(
            1 for item in response['items']
            if item.get('delete', {}).get('error')
            and item['delete']['error'].get('type') != 'not_found'
        )
    return {"deleted": len(ids) - failed, "failed": failed}


def handler(event, context):
    run_start      = datetime.now(timezone.utc)
    last_synced_at = load_watermark()
    environment    = os.environ.get('ENVIRONMENT', 'unknown')
    print(f"[INFO] Checking for rows updated after: {last_synced_at}")

    pg_conn   = get_pg_connection()
    os_client = get_os_client()

    total_indexed = 0
    total_deleted = 0
    total_failed  = 0
    rows_fetched  = 0
    new_watermark = last_synced_at

    try:
        cursor = pg_conn.cursor(name='incremental_sync_cursor')
        cursor.itersize = 200
        cursor.execute(f"""
            SELECT * FROM {POSTGRES_TABLE}
            WHERE updated_at > %s
            ORDER BY updated_at ASC
        """, (last_synced_at,))

        upsert_batch, upsert_bytes = [], 0
        delete_batch = []

        for row in cursor:
            rows_fetched += 1
            doc = {
                desc[0]: (val.isoformat() if isinstance(val, datetime) else val)
                for desc, val in zip(cursor.description, row)
            }

            if doc.get('updated_at'):
                new_watermark = doc['updated_at']

            # Soft-delete: if deleted_at is set, remove from OpenSearch
            if doc.get('deleted_at') is not None:
                delete_batch.append(doc['id'])
                if len(delete_batch) >= BATCH_SIZE:
                    result = bulk_delete(os_client, delete_batch)
                    total_deleted += result['deleted']
                    total_failed  += result['failed']
                    delete_batch = []
            else:
                doc_size = len(json.dumps(doc, default=str).encode('utf-8'))
                if upsert_batch and (len(upsert_batch) >= BATCH_SIZE or upsert_bytes + doc_size > MAX_BYTES):
                    result = bulk_upsert(os_client, upsert_batch)
                    total_indexed += result['indexed']
                    total_failed  += result['failed']
                    upsert_batch, upsert_bytes = [], 0
                upsert_batch.append(doc)
                upsert_bytes += doc_size

        if upsert_batch:
            result = bulk_upsert(os_client, upsert_batch)
            total_indexed += result['indexed']
            total_failed  += result['failed']

        if delete_batch:
            result = bulk_delete(os_client, delete_batch)
            total_deleted += result['deleted']
            total_failed  += result['failed']

        cursor.close()

    except Exception as e:
        print(f"[ERROR] Sync failed: {e}")
        alert(
            f"OpenSearch Sync Failed [{environment}]",
            f"Incremental sync failed in {environment} environment.\n\nError: {e}"
        )
        pg_conn.close()
        raise e
    finally:
        pg_conn.close()

    if rows_fetched > 0:
        save_watermark(new_watermark)
        print(f"[INFO] Watermark updated to: {new_watermark}")
    else:
        print("[INFO] No new rows found.")

    if total_failed > 0:
        alert(
            f"OpenSearch Sync Partial Failure [{environment}]",
            f"Sync completed with {total_failed} failed documents in {environment}."
        )

    duration = (datetime.now(timezone.utc) - run_start).total_seconds()
    summary  = {
        "rows_fetched":  rows_fetched,
        "total_indexed": total_indexed,
        "total_deleted": total_deleted,
        "total_failed":  total_failed,
        "new_watermark": new_watermark,
        "duration_secs": round(duration, 2)
    }
    print(f"[DONE] {json.dumps(summary)}")
    return summary
