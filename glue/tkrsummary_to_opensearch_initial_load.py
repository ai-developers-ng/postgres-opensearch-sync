"""
Glue Python Shell Job: tkrsummary_to_opensearch_initial_load
Reads all rows from PostgreSQL tkrsummary, and bulk-indexes into
the OpenSearch 'records' index. Writes a watermark checkpoint at
the end so RecordsSyncLambda picks up only rows created after this run.
"""

import json
import os
import sys
import argparse
import boto3
import psycopg
import psycopg.rows
from datetime import datetime, timezone
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

parser = argparse.ArgumentParser()
parser.add_argument('--JOB_NAME',             default='tkrsummary-to-opensearch-initial-load')
parser.add_argument('--postgres_host',        required=True)
parser.add_argument('--postgres_port',        required=True)
parser.add_argument('--postgres_db',          required=True)
parser.add_argument('--postgres_secret_arn',  required=True)
parser.add_argument('--opensearch_endpoint',  required=True)
parser.add_argument('--opensearch_index',     required=True)
parser.add_argument('--aws_region',           required=True)
parser.add_argument('--checkpoint_bucket',    required=True)
parsed, _ = parser.parse_known_args()
args = vars(parsed)

REGION      = args['aws_region']
OS_ENDPOINT = args['opensearch_endpoint'].replace('https://', '')
INDEX_NAME  = args['opensearch_index']
BATCH_SIZE  = 500
MAX_BYTES   = 5_000_000

sm_client = boto3.client('secretsmanager', region_name=REGION)


def get_secret(arn: str) -> dict:
    return json.loads(sm_client.get_secret_value(SecretId=arn)['SecretString'])


def get_pg_conn():
    creds = get_secret(args['postgres_secret_arn'])
    return psycopg.connect(
        host=args['postgres_host'],
        port=int(args['postgres_port']),
        dbname=args['postgres_db'],
        user=creds['username'],
        password=creds['password'],
        connect_timeout=30,
        row_factory=psycopg.rows.dict_row,
    )


def get_os_client():
    creds = boto3.Session().get_credentials().get_frozen_credentials()
    auth = AWS4Auth(
        creds.access_key, creds.secret_key,
        REGION, 'aoss', session_token=creds.token,
    )
    return OpenSearch(
        hosts=[{'host': OS_ENDPOINT, 'port': 443}],
        http_auth=auth, use_ssl=True, verify_certs=True,
        connection_class=RequestsHttpConnection, timeout=60,
    )


def ensure_index(os_client):
    if not os_client.indices.exists(index=INDEX_NAME):
        os_client.indices.create(index=INDEX_NAME, body={
            'settings': {
                'index': {
                    'refresh_interval': '-1',
                    'number_of_replicas': 0,
                    'number_of_shards': 5,
                }
            },
            'mappings': {
                'properties': {
                    'tkrid':       {'type': 'keyword'},
                    'jsonsummary': {'type': 'object', 'enabled': True},
                    'createddt':   {'type': 'date'},
                    'updateddt':   {'type': 'date'},
                    'createdby':   {'type': 'keyword'},
                    'updatedby':   {'type': 'keyword'},
                }
            },
        })
        print(f'[INFO] Created index: {INDEX_NAME}')
    else:
        print(f'[INFO] Index already exists: {INDEX_NAME}, disabling refresh for bulk load')
        os_client.indices.put_settings(
            index=INDEX_NAME,
            body={'index': {'refresh_interval': '-1', 'number_of_replicas': 0}},
        )


def flush_batch(os_client, batch: list) -> tuple:
    body = []
    for doc in batch:
        body.append({'index': {'_index': INDEX_NAME, '_id': str(doc['tkrid'])}})
        body.append(doc)
    resp = os_client.bulk(body=body)
    indexed, failed = len(batch), 0
    if resp.get('errors'):
        failed = sum(1 for item in resp['items'] if item.get('index', {}).get('error'))
        indexed -= failed
        print(f'[WARN] {failed} docs failed in this batch')
    return indexed, failed


def run():
    print(f'[INFO] Starting tkrsummary → OpenSearch initial load at {datetime.now(timezone.utc).isoformat()}')

    os_client = get_os_client()
    ensure_index(os_client)

    pg_conn       = get_pg_conn()
    total_indexed = 0
    total_failed  = 0
    last_tkrid    = None
    batch         = []
    batch_bytes   = 0

    try:
        while True:
            with pg_conn.cursor() as cursor:
                if last_tkrid is None:
                    cursor.execute(
                        'SELECT tkrid, jsonsummary, createddt, createdby, updateddt, updatedby '
                        'FROM "TKR".tkrsummary '
                        'ORDER BY tkrid ASC '
                        'LIMIT %s',
                        (BATCH_SIZE,),
                    )
                else:
                    cursor.execute(
                        'SELECT tkrid, jsonsummary, createddt, createdby, updateddt, updatedby '
                        'FROM "TKR".tkrsummary '
                        'WHERE tkrid > %s '
                        'ORDER BY tkrid ASC '
                        'LIMIT %s',
                        (last_tkrid, BATCH_SIZE),
                    )
                rows = cursor.fetchall()

            if not rows:
                break

            for row in rows:
                # Parse jsonsummary string → dict so OpenSearch stores it as a proper object
                json_summary = row['jsonsummary']
                if isinstance(json_summary, str):
                    try:
                        json_summary = json.loads(json_summary)
                    except Exception:
                        json_summary = {}

                doc = {
                    'tkrid':       str(row['tkrid']),
                    'jsonsummary': json_summary,
                    'createddt':   row['createddt'].isoformat() if row['createddt'] else None,
                    'updateddt':   row['updateddt'].isoformat() if row['updateddt'] else None,
                    'createdby':   row['createdby'],
                    'updatedby':   row['updatedby'],
                }

                doc_size = len(json.dumps(doc, default=str).encode('utf-8'))
                if batch and (len(batch) >= BATCH_SIZE or batch_bytes + doc_size > MAX_BYTES):
                    indexed, failed = flush_batch(os_client, batch)
                    total_indexed += indexed
                    total_failed  += failed
                    batch, batch_bytes = [], 0

                batch.append(doc)
                batch_bytes += doc_size

            last_tkrid = str(rows[-1]['tkrid'])
            print(f'[INFO] Processed up to tkrid={last_tkrid}, total_indexed={total_indexed}')

            if len(rows) < BATCH_SIZE:
                break

        # Flush last batch
        if batch:
            indexed, failed = flush_batch(os_client, batch)
            total_indexed += indexed
            total_failed  += failed

    except Exception as e:
        print(f'[ERROR] Initial load failed: {e}')
        raise
    finally:
        pg_conn.close()

    # Restore index settings after bulk load
    os_client.indices.put_settings(
        index=INDEX_NAME,
        body={'index': {'refresh_interval': '30s', 'number_of_replicas': 1}},
    )
    os_client.indices.forcemerge(index=INDEX_NAME, max_num_segments=5)
    print('[INFO] Index settings restored and force merge triggered')

    # Seed watermark so RecordsSyncLambda picks up only rows after this run
    s3 = boto3.client('s3', region_name=REGION)
    watermark = {'last_synced_at': datetime.now(timezone.utc).isoformat()}
    s3.put_object(
        Bucket=args['checkpoint_bucket'],
        Key='checkpoints/tkrsummary_watermark.json',
        Body=json.dumps(watermark),
    )
    print(f'[INFO] Watermark seeded: {watermark}')
    print(f'[DONE] Initial load complete. total_indexed={total_indexed}, total_failed={total_failed}')


run()
