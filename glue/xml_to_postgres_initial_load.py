"""
Glue Python Shell Job: xml_to_postgres_initial_load
Reads all rows from MariaDB TKRSUMMARY, parses SUMMARYCLOB XML → JSON,
and bulk-upserts into PostgreSQL tkrsummary table.
"""

import json
import sys
import boto3
import pymysql
import psycopg2
import psycopg2.extras
import xmltodict
from awsglue.utils import getResolvedOptions
from datetime import datetime, timezone

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'mariadb_secret_arn',
    'postgres_host', 'postgres_port', 'postgres_db', 'postgres_secret_arn',
    'aws_region',
])

REGION         = args['aws_region']
BATCH_SIZE     = 1000   # rows fetched from MariaDB per round-trip
PG_PAGE_SIZE   = 500    # rows per VALUES(...) statement in execute_values
sm_client      = boto3.client('secretsmanager', region_name=REGION)


def get_secret(arn: str) -> dict:
    return json.loads(sm_client.get_secret_value(SecretId=arn)['SecretString'])


def get_mariadb_conn():
    creds = get_secret(args['mariadb_secret_arn'])
    return pymysql.connect(
        host=creds['host'],
        user=creds['username'],
        password=creds['password'],
        database=creds['dbname'],
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_pg_conn():
    creds = get_secret(args['postgres_secret_arn'])
    return psycopg2.connect(
        host=args['postgres_host'],
        port=int(args['postgres_port']),
        dbname=args['postgres_db'],
        user=creds['username'],
        password=creds['password'],
        connect_timeout=10,
        options='-c statement_timeout=120000',
    )


UPSERT_SQL = """
    INSERT INTO tkrsummary (tkrid, jsonsummary, createddt, createdby, updateddt, updatedby)
    VALUES %s
    ON CONFLICT (tkrid) DO UPDATE
        SET jsonsummary = EXCLUDED.jsonsummary,
            updateddt   = EXCLUDED.updateddt,
            updatedby   = EXCLUDED.updatedby
"""


def parse_xml_to_json(xml_text: str) -> str:
    """Parse XML string to JSON string. Returns '{}' on failure."""
    try:
        parsed = xmltodict.parse(xml_text, force_list=False)
        return json.dumps(parsed)
    except Exception as e:
        print(f"[WARN] XML parse error: {e}")
        return '{}'


def run():
    print(f"[INFO] Starting XML to PostgreSQL initial load at {datetime.now(timezone.utc).isoformat()}")

    mariadb_conn = get_mariadb_conn()
    pg_conn      = get_pg_conn()
    pg_cursor    = pg_conn.cursor()

    total_rows    = 0
    total_skipped = 0

    # Keyset pagination cursors — avoids O(n²) OFFSET scanning.
    # We page on (CREATE_DATE, TKRID) so each query uses an index seek
    # rather than scanning and discarding all prior rows.
    last_create_date = None
    last_tkrid       = None

    try:
        while True:
            with mariadb_conn.cursor() as cursor:
                if last_create_date is None:
                    # First page — no prior cursor
                    cursor.execute(
                        "SELECT TKRID, CREATE_DATE, SUMMARYCLOB "
                        "FROM TKRSUMMARY "
                        "ORDER BY CREATE_DATE ASC, TKRID ASC "
                        "LIMIT %s",
                        (BATCH_SIZE,),
                    )
                else:
                    # Subsequent pages — seek past last seen (CREATE_DATE, TKRID).
                    # The OR handles ties on CREATE_DATE correctly.
                    cursor.execute(
                        "SELECT TKRID, CREATE_DATE, SUMMARYCLOB "
                        "FROM TKRSUMMARY "
                        "WHERE CREATE_DATE > %s "
                        "   OR (CREATE_DATE = %s AND TKRID > %s) "
                        "ORDER BY CREATE_DATE ASC, TKRID ASC "
                        "LIMIT %s",
                        (last_create_date, last_create_date, last_tkrid, BATCH_SIZE),
                    )
                rows = cursor.fetchall()

            if not rows:
                break

            records = []
            for row in rows:
                tkrid       = row['TKRID']
                create_date = row['CREATE_DATE']
                xml_text    = row['SUMMARYCLOB'] or ''

                if not xml_text.strip():
                    print(f"[WARN] Empty SUMMARYCLOB for TKRID={tkrid}, skipping")
                    total_skipped += 1
                    continue

                json_str = parse_xml_to_json(xml_text)
                records.append((
                    str(tkrid),
                    json_str,
                    create_date,
                    'glue-initial-load',
                    datetime.now(timezone.utc),
                    'glue-initial-load',
                ))

            # Advance the keyset cursor to the last row of this batch
            last_row         = rows[-1]
            last_create_date = last_row['CREATE_DATE']
            last_tkrid       = last_row['TKRID']

            if records:
                psycopg2.extras.execute_values(
                    pg_cursor, UPSERT_SQL, records, template=None, page_size=PG_PAGE_SIZE
                )
                pg_conn.commit()

            total_rows += len(records)
            print(
                f"[INFO] Batch done: upserted={len(records)}, skipped={BATCH_SIZE - len(records) - (BATCH_SIZE - len(rows))}, "
                f"total_upserted={total_rows}, cursor=({last_create_date}, {last_tkrid})"
            )

            if len(rows) < BATCH_SIZE:
                break

    except Exception as e:
        pg_conn.rollback()
        print(f"[ERROR] Initial load failed: {e}")
        raise
    finally:
        pg_cursor.close()
        pg_conn.close()
        mariadb_conn.close()

    print(f"[DONE] Initial load complete. Total upserted={total_rows}, skipped={total_skipped}")


run()
