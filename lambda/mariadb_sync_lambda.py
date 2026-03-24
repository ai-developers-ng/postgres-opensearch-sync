"""
Lambda: mariadb_sync_lambda
Polls MariaDB TKRSUMMARY every ~30 seconds (two cycles per 1-minute EventBridge trigger),
parses SUMMARYCLOB XML → JSON, and upserts into PostgreSQL tkrsummary.
"""

import json
import os
import time
import boto3
import pymysql
import psycopg
import xmltodict
from datetime import datetime, timezone

REGION            = os.environ['AWS_REGION']
MARIADB_SECRET_ARN = os.environ['MARIADB_SECRET_ARN']
POSTGRES_HOST     = os.environ['POSTGRES_HOST']
POSTGRES_PORT     = int(os.environ.get('POSTGRES_PORT', 5445))
POSTGRES_DB       = os.environ['POSTGRES_DB']
POSTGRES_SECRET_ARN = os.environ['POSTGRES_SECRET_ARN']
CHECKPOINT_BUCKET = os.environ['CHECKPOINT_BUCKET']
CHECKPOINT_KEY    = 'checkpoints/mariadb_sync_checkpoint.json'
MARIADB_PORT      = int(os.environ.get('MARIADB_PORT', 3306))
BATCH_LIMIT       = 500
ALERT_TOPIC_ARN   = os.environ.get('ALERT_TOPIC_ARN')

s3_client  = boto3.client('s3')
sm_client  = boto3.client('secretsmanager', region_name=REGION)
sns_client = boto3.client('sns', region_name=REGION) if ALERT_TOPIC_ARN else None

_mariadb_creds = None
_pg_creds      = None


def _get_secret(arn: str) -> dict:
    return json.loads(sm_client.get_secret_value(SecretId=arn)['SecretString'])


def _get_mariadb_creds() -> dict:
    global _mariadb_creds
    if _mariadb_creds is None:
        _mariadb_creds = _get_secret(MARIADB_SECRET_ARN)
    return _mariadb_creds


def _get_pg_creds() -> dict:
    global _pg_creds
    if _pg_creds is None:
        _pg_creds = _get_secret(POSTGRES_SECRET_ARN)
    return _pg_creds


def _mariadb_conn():
    creds = _get_mariadb_creds()
    return pymysql.connect(
        host=creds['host'],
        port=MARIADB_PORT,
        user=creds['username'],
        password=creds['password'],
        database=creds['dbname'],
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _pg_conn():
    creds = _get_pg_creds()
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=creds['username'],
        password=creds['password'],
        connect_timeout=10,
        options='-c statement_timeout=30000',
    )


def _load_checkpoint() -> str:
    try:
        obj = s3_client.get_object(Bucket=CHECKPOINT_BUCKET, Key=CHECKPOINT_KEY)
        return json.loads(obj['Body'].read())['last_create_date']
    except s3_client.exceptions.NoSuchKey:
        # First run: use epoch to catch everything
        fallback = '1970-01-01T00:00:00'
        print(f"[INFO] No checkpoint found, starting from {fallback}")
        return fallback
    except Exception as e:
        print(f"[WARN] Could not load checkpoint: {e}, starting from epoch")
        return '1970-01-01T00:00:00'


def _save_checkpoint(last_create_date: str):
    s3_client.put_object(
        Bucket=CHECKPOINT_BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps({'last_create_date': last_create_date}),
    )


def _parse_xml(xml_text: str, tkrid: str) -> str:
    try:
        parsed = xmltodict.parse(xml_text, force_list=False)
        return json.dumps(parsed)
    except Exception as e:
        print(f"[WARN] XML parse error for TKRID={tkrid}: {e}")
        return '{}'


UPSERT_SQL = """
    INSERT INTO tkrsummary (tkrid, jsonsummary, createddt, createdby, updateddt, updatedby)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (tkrid) DO UPDATE
        SET jsonsummary = EXCLUDED.jsonsummary,
            updateddt   = EXCLUDED.updateddt,
            updatedby   = EXCLUDED.updatedby
"""


def _alert(subject: str, message: str):
    if sns_client and ALERT_TOPIC_ARN:
        try:
            sns_client.publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject, Message=message)
        except Exception as e:
            print(f"[WARN] SNS publish failed: {e}")


def _sync_cycle(cycle_num: int) -> dict:
    """Run one sync cycle: read new MariaDB rows → upsert PostgreSQL."""
    checkpoint   = _load_checkpoint()
    print(f"[INFO] Cycle {cycle_num}: checking rows with CREATE_DATE > {checkpoint}")

    mariadb = _mariadb_conn()
    pg      = _pg_conn()
    pg_cur  = pg.cursor()

    rows_fetched  = 0
    rows_upserted = 0
    rows_skipped  = 0
    last_date     = checkpoint

    try:
        with mariadb.cursor() as cursor:
            cursor.execute(
                "SELECT TKRID, CREATE_DATE, SUMMARYCLOB "
                "FROM TKRSUMMARY "
                "WHERE CREATE_DATE > %s "
                "ORDER BY CREATE_DATE ASC "
                "LIMIT %s",
                (checkpoint, BATCH_LIMIT),
            )
            rows = cursor.fetchall()

        rows_fetched = len(rows)

        if rows:
            records = []
            for row in rows:
                tkrid    = str(row['TKRID'])
                xml_text = row['SUMMARYCLOB'] or ''
                create_date = row['CREATE_DATE']

                if isinstance(create_date, datetime):
                    last_date = create_date.isoformat()
                else:
                    last_date = str(create_date)

                if not xml_text.strip():
                    print(f"[WARN] Empty SUMMARYCLOB for TKRID={tkrid}, skipping")
                    rows_skipped += 1
                    continue

                json_str = _parse_xml(xml_text, tkrid)
                records.append((
                    tkrid,
                    json_str,
                    create_date,
                    'mariadb-sync',
                    datetime.now(timezone.utc),
                    'mariadb-sync',
                ))

            if records:
                pg_cur.executemany(UPSERT_SQL, records)
                pg.commit()
                rows_upserted = len(records)
                _save_checkpoint(last_date)
                print(f"[INFO] Cycle {cycle_num}: upserted {rows_upserted} rows, new checkpoint={last_date}")
        else:
            print(f"[INFO] Cycle {cycle_num}: no new rows found")

    except Exception as e:
        pg.rollback()
        print(f"[ERROR] Cycle {cycle_num} failed: {e}")
        raise
    finally:
        pg_cur.close()
        pg.close()
        mariadb.close()

    return {
        'rows_fetched':  rows_fetched,
        'rows_upserted': rows_upserted,
        'rows_skipped':  rows_skipped,
    }


def handler(event, context):
    environment = os.environ.get('ENVIRONMENT', 'unknown')
    run_start   = datetime.now(timezone.utc)
    totals      = {'rows_fetched': 0, 'rows_upserted': 0, 'rows_skipped': 0}

    try:
        # Cycle 1
        result = _sync_cycle(1)
        for k in totals:
            totals[k] += result[k]

        # Wait 28 seconds then run cycle 2 (total ~30s cadence)
        time.sleep(28)

        # Cycle 2
        result = _sync_cycle(2)
        for k in totals:
            totals[k] += result[k]

    except Exception as e:
        _alert(
            f"MariaDB Sync Failed [{environment}]",
            f"mariadb-sync Lambda failed in {environment}.\n\nError: {e}",
        )
        raise

    duration = round((datetime.now(timezone.utc) - run_start).total_seconds(), 2)
    summary  = {**totals, 'duration_secs': duration}
    print(f"[DONE] {json.dumps(summary)}")
    return summary
