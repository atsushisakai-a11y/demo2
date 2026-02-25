import os
import time
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# =========================
# Parameters you requested
# =========================
PROJECT_ID = "eneco-488308"
DATASET_ID = "raw"
TABLE_NAME = "raw_google_poi"
LOCATION = "Rotterdam, Netherlands"

KEYWORDS = [
    "bakery",
    "butcher",
    "fashion store",
    "restaurant",
    "cafe",
    "lunchroom",
    "salon",
    "beauty services",
    "ICT repair shop",
]

# ====== TEST LIMITS (cost control) ======
MAX_API_CALLS_TOTAL = 10          # ✅ hard cap on Places requests
MAX_RESULTS_PER_KEYWORD = 10      # optional: keep inserts small too
SLEEP_BETWEEN_CALLS_SEC = 0.2

API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing GOOGLE_MAPS_API_KEY env var (use GitHub Secrets/env).")

TEXTSEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_row_id(location: str, keyword: str, place_id: str) -> str:
    raw = f"{location}||{keyword}||{place_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def fetch_places_text_search_single_call(query: str) -> List[Dict[str, Any]]:
    """
    One single Text Search call. No pagination (keeps cost predictable).
    """
    params = {"query": query, "key": API_KEY}
    resp = requests.get(TEXTSEARCH_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    status = payload.get("status")
    if status not in ("OK", "ZERO_RESULTS"):
        raise RuntimeError(f"Places API error: status={status}, payload={payload}")

    return payload.get("results", [])


def ensure_bq_table(client: bigquery.Client, table_fqdn: str) -> None:
    """
    TEST schema: store raw payload as STRING (raw_json) to avoid JSON/RECORD mismatch.
    If your table already exists with different schema, we do NOT try to alter it here.
    """
    schema = [
        bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("row_id", "STRING", mode="REQUIRED"),

        bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("formatted_address", "STRING"),
        bigquery.SchemaField("business_status", "STRING"),
        bigquery.SchemaField("types", "STRING", mode="REPEATED"),
        bigquery.SchemaField("rating", "FLOAT"),
        bigquery.SchemaField("user_ratings_total", "INTEGER"),
        bigquery.SchemaField("price_level", "INTEGER"),
        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lng", "FLOAT"),

        bigquery.SchemaField("raw_json", "STRING"),  # ✅ safe for test
    ]

    try:
        client.get_table(table_fqdn)
    except NotFound:
        # Create dataset if needed
        dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            client.create_dataset(dataset_ref)

        table = bigquery.Table(table_fqdn, schema=schema)
        client.create_table(table)


def to_bq_rows(location: str, keyword: str, places: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fetched_at = now_utc_iso()
    rows: List[Dict[str, Any]] = []

    for p in places[:MAX_RESULTS_PER_KEYWORD]:
        place_id = p.get("place_id")
        if not place_id:
            continue

        geom = (p.get("geometry") or {}).get("location") or {}
        lat = geom.get("lat")
        lng = geom.get("lng")

        rows.append(
            {
                "fetched_at": fetched_at,
                "location": location,
                "keyword": keyword,
                "row_id": stable_row_id(location, keyword, place_id),

                "place_id": place_id,
                "name": p.get("name"),
                "formatted_address": p.get("formatted_address"),
                "business_status": p.get("business_status"),
                "types": p.get("types", []) or [],
                "rating": p.get("rating"),
                "user_ratings_total": p.get("user_ratings_total"),
                "price_level": p.get("price_level"),
                "lat": lat,
                "lng": lng,

                # store raw as JSON string to avoid BigQuery type mismatch
                "raw_json": json.dumps(p, ensure_ascii=False),
            }
        )

    return rows


def insert_rows(client: bigquery.Client, table_fqdn: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    errors = client.insert_rows_json(table_fqdn, rows)
    if errors:
        # show only first few errors to keep logs readable
        raise RuntimeError(f"BigQuery insert errors (sample): {errors[:3]}")


def main() -> None:
    bq = bigquery.Client(project=PROJECT_ID)
    table_fqdn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    ensure_bq_table(bq, table_fqdn)

    api_calls = 0
    total_inserted = 0

    for kw in KEYWORDS:
        if api_calls >= MAX_API_CALLS_TOTAL:
            print(f"[STOP] Reached MAX_API_CALLS_TOTAL={MAX_API_CALLS_TOTAL}")
            break

        query = f"{kw} in {LOCATION}"
        places = fetch_places_text_search_single_call(query=query)
        api_calls += 1

        rows = to_bq_rows(location=LOCATION, keyword=kw, places=places)
        insert_rows(bq, table_fqdn, rows)

        print(f"[OK] call#{api_calls} keyword='{kw}' fetched={len(places)} inserted={len(rows)}")
        total_inserted += len(rows)

        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    print(f"Done. API calls: {api_calls}, total inserted rows: {total_inserted}")


if __name__ == "__main__":
    main()
