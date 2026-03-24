import base64
import json
import os

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

REGION = os.environ["AWS_REGION"]
OS_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"].lstrip("https://")
OS_INDEX = os.environ["OPENSEARCH_INDEX"]

_ALLOWED_INDICES = {OS_INDEX, "records"}


def _build_os_client():
    credentials = boto3.Session().get_credentials().get_frozen_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        REGION,
        "aoss",
        session_token=credentials.token,
    )
    return OpenSearch(
        hosts=[{"host": OS_ENDPOINT, "port": 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30,
    )


_os_client = _build_os_client()


def _parse_request_payload(event: dict) -> dict:
    if not isinstance(event, dict):
        return {}

    body = event.get("body")
    if isinstance(body, str) and body:
        try:
            if event.get("isBase64Encoded"):
                body = base64.b64decode(body).decode("utf-8")
            return json.loads(body)
        except Exception:
            return {}

    if isinstance(body, dict):
        return body

    return event


def _extract_search_params(event: dict) -> tuple[str, int, str]:
    payload = _parse_request_payload(event)
    query_params = event.get("queryStringParameters") or {}

    query = (
        query_params.get("q")
        or payload.get("q")
        or payload.get("query")
        or payload.get("search")
        or ""
    )

    size_raw = query_params.get("size") or payload.get("size") or 10
    try:
        size = int(size_raw)
    except (TypeError, ValueError):
        size = 10
    size = max(1, min(size, 100))

    index_raw = str(query_params.get("index") or payload.get("index") or "").strip()
    index = index_raw if index_raw in _ALLOWED_INDICES else OS_INDEX

    return str(query).strip(), size, index


def handler(event, context):
    query, size, index = _extract_search_params(event if isinstance(event, dict) else {})

    if not query:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Missing search query. Provide 'q' or 'query'."}),
        }

    try:
        response = _os_client.search(
            index=index,
            body={
                "size": size,
                "query": {
                    "query_string": {
                        "query": query,
                        "fields": ["*"],
                        "default_operator": "AND",
                        "fuzziness": "AUTO",
                        "lenient": True,
                    }
                },
            },
        )
    except Exception as exc:
        return {
            "statusCode": 502,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Search backend unavailable.", "detail": str(exc)}),
        }

    hits_section = response.get("hits", {})
    raw_total = hits_section.get("total", 0)
    total_hits = raw_total.get("value", 0) if isinstance(raw_total, dict) else int(raw_total)
    max_score = hits_section.get("max_score") or 0.0

    results = []
    for hit in hits_section.get("hits", []):
        score = float(hit.get("_score") or 0.0)
        matching_percentage = round((score / max_score) * 100, 2) if max_score > 0 else 0.0
        results.append(
            {
                "id": hit.get("_id"),
                "score": score,
                "matching_percentage": matching_percentage,
                "source": hit.get("_source", {}),
            }
        )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(
            {
                "query": query,
                "total_hits": total_hits,
                "results": results,
            }
        ),
    }
