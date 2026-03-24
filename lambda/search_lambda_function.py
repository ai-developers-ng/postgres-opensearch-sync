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


def _extract_search_params(event: dict) -> tuple[str, int, str, dict, dict]:
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

    # must_match: {field: value} — each field must match 100% (match_phrase)
    raw_must_match = payload.get("must_match")
    must_match = {}
    if isinstance(raw_must_match, dict):
        for k, v in raw_must_match.items():
            k = str(k).strip()
            v = str(v).strip()
            if k and v:
                must_match[k] = v

    # date_filters: {field: {gte: "...", lte: "..."}} — applied as range filter
    raw_date_filters = payload.get("date_filters")
    date_filters = {}
    if isinstance(raw_date_filters, dict):
        for field, bounds in raw_date_filters.items():
            field = str(field).strip()
            if not field or not isinstance(bounds, dict):
                continue
            range_clause = {}
            for op in ("gte", "lte", "gt", "lt"):
                if op in bounds and bounds[op] is not None:
                    range_clause[op] = str(bounds[op]).strip()
            if range_clause:
                date_filters[field] = range_clause

    return str(query).strip(), size, index, must_match, date_filters


def _build_os_query(query: str, must_match: dict, date_filters: dict) -> dict:
    """
    Unstructured (no must_match/date_filters): simple query_string with fuzziness.
    Structured (must_match or date_filters present): bool query with:
      - must: query_string for keyword spread + match_phrase per exact-match field
      - filter: range per date field
    """
    if not must_match and not date_filters:
        return {
            "query_string": {
                "query": query,
                "fields": ["*"],
                "default_operator": "AND",
                "lenient": True,
            }
        }

    must = []
    if query:
        must.append({
            "query_string": {
                "query": query,
                "fields": ["*"],
                "default_operator": "AND",
                "lenient": True,
            }
        })
    for field, value in must_match.items():
        must.append({"match_phrase": {field: value}})

    filters = [{"range": {field: bounds}} for field, bounds in date_filters.items()]

    bool_clause = {}
    if must:
        bool_clause["must"] = must
    if filters:
        bool_clause["filter"] = filters

    return {"bool": bool_clause}


def handler(event, context):
    query, size, index, must_match, date_filters = _extract_search_params(
        event if isinstance(event, dict) else {}
    )

    if not query and not must_match and not date_filters:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "error": "Missing search criteria. Provide 'q', 'must_match', or 'date_filters'."
            }),
        }

    os_query = _build_os_query(query, must_match, date_filters)

    try:
        response = _os_client.search(
            index=index,
            body={"size": size, "query": os_query},
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
                "must_match": must_match if must_match else None,
                "date_filters": date_filters if date_filters else None,
                "index": index,
                "total_hits": total_hits,
                "results": results,
            }
        ),
    }
