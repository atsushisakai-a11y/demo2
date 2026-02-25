import os
import time
import json
import math
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Iterable, Tuple

import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# =========================
# Parameters
# =========================
PROJECT_ID = "eneco-488308"
DATASET_ID = "raw"
TABLE_NAME = "raw_google_poi"

CITY_LABEL = "Rotterdam, Netherlands"

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

# =========================
# Coverage config (approx Rotterdam bounding box)
# You can widen if you want Rotterdam metro coverage.
# =========================
ROTTERDAM_BBOX = {
    "min_lat": 51.87,
    "max_lat": 52.02,
    "min_lng": 4.35,
    "max_lng": 4.60,
}

# Nearby Search radius in meters.
# 1500–2500 is a good practical range; larger can miss things / bias results.
RADIUS_M = 2000

# Grid step in meters (smaller step = more calls, better coverage).
# A common heuristic: step ~ radius * 0.8 to ensure overlap.
GRID_STEP_M = int(RADIUS_M * 0.8)

# Pagination: Nearby Search returns up to 20 results/page, max 3 pages.
MAX_PAGES_PER_TILE = 3

# BigQuery inserts
BQ_BATCH_SIZE = 500

# Throttling
SLEEP_BETWEEN_CALLS_SEC = 0.2
SLEEP_FOR_NEXT_PAGE_TOKEN_SEC = 2.0

# =========================
# Auth
# =========================
API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing GOOGLE_MAPS_API_KEY env var (use GitHub Secrets/env).")

NEARBYSEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_row_id(location: str, keyword: str, place_id: str) -> str:
    raw = f"{location}||{keyword}||{place_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def meters_to_lat_deg(m: float) -> float:
    return m / 111_320.0


def meters_to_lng_deg(m: float, lat_deg: float) -> float:
    # Adjust for longitude shrinkage by latitude
    return m / (111_320.0 * math.cos(math.radians(lat_deg)))


def generate_grid_points(bbox: Dict[str, float], step_m: int) -> Iterable[Tuple[float, float]]:
    """
    Generates a lat/lng grid covering bbox with ~step_m spacing.
    """
    min_lat, max_lat = bbox["min_lat"], bbox["max_lat"]
    min_lng, max_lng = bbox["min_lng"], bbox["max_lng"]

    # Use mid-lat to compute longitude degree step
    mid_lat = (min_lat + max_lat) / 2
    lat_step = meters_to_lat_deg(step_m)
    lng_step = meters_to_lng_deg(step_m, mid_lat)

    lat = min_lat
    while lat <= max_lat:
        lng = min_lng
        while lng <= max_lng:
            yield (round(lat, 6), round(lng, 6))
            lng += lng_step
        lat += lat_step


def fetch_nearby_search_all_pages(lat: float, lng: float, keyword: str) -> List[Dict[str, Any]]:
    """
    Fetch Nearby Search results with pagination (up to MAX_PAGES_PER_TILE pages).
    """
    all_results: List[Dict[str, Any]] = []
    next_token = None

    for page in range(MAX_PAGES_PER_TILE):
        if next_token:
            time.sleep(SLEEP_FOR_NEXT_PAGE_TOKEN_SEC)
            params = {"pagetoken": next_token, "key": API_KEY}
        else:
            params = {
                "location": f"{lat},{lng}",
                "radius": RADIUS_M,
                "keyword": keyword,
                "key": API_KEY,
            }

        resp = requests.get(NEARBYSEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        status = payload.get("status")
        if status not in ("OK", "ZERO_RESULTS"):
            raise RuntimeError(f"Places API error: status={status}, payload={payload}")

        all_results.extend(payload.get("results", []))
        next_token = payload.get("next_page_token")
        if not next_token:
            break

    return all_results


def ensure_bq_table(client: bigquery.Client, table_fqdn: str) -> None:
    schema = [
        bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("keyword", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("row_id", "STRING", mode="REQUIRED"),

        bigquery.SchemaField("tile_lat", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("tile_lng", "FLOAT", mode="REQUIRED"),

        bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("vicinity", "STRING"),
        bigquery.SchemaField("business_status", "STRING"),
        bigquery.SchemaField("types", "STRING", mode="REPEATED"),

        bigquery.SchemaField("rating", "FLOAT"),
        bigquery.SchemaField("user_ratings_total", "INTEGER"),
        bigquery.SchemaField("price_level", "INTEGER"),

        bigquery.SchemaField("lat", "FLOAT"),
        bigquery.SchemaField("lng", "FLOAT"),

        bigquery.SchemaField("raw_json", "STRING"),
    ]

    try:
        client.get_table(table_fqdn)
    except NotFound:
        dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        try:
            client.get_dataset(dataset_ref)
        except NotFound:
            client.create_dataset(dataset_ref)

        table = bigquery.Table(table_fqdn, schema=schema)
        client.create_table(table)


def to_bq_rows(
    location_label: str,
    keyword: str,
    tile_lat: float,
    tile_lng: float,
    places: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    fetched_at = now_utc_iso()
    rows: List[Dict[str, Any]] = []

    for p in places:
        place_id = p.get("place_id")
        if not place_id:
            continue

        geom = (p.get("geometry") or {}).get("location") or {}
        lat = geom.get("lat")
        lng = geom.get("lng")

        rows.append(
            {
                "fetched_at": fetched_at,
                "location": location_label,
                "keyword": keyword,
                "row_id": stable_row_id(location_label, keyword, place_id),

                "tile_lat": tile_lat,
                "tile_lng": tile_lng,

                "place_id": place_id,
                "name": p.get("name"),
                "vicinity": p.get("vicinity"),
                "business_status": p.get("business_status"),
                "types": p.get("types", []) or [],

                "rating": p.get("rating"),
                "user_ratings_total": p.get("user_ratings_total"),
                "price_level": p.get("price_level"),

                "lat": lat,
                "lng": lng,

                "raw_json": json.dumps(p, ensure_ascii=False),
            }
        )

    return rows


def insert_rows_batched(client: bigquery.Client, table_fqdn: str, rows: List[Dict[str, Any]]) -> None:
    for i in range(0, len(rows), BQ_BATCH_SIZE):
        chunk = rows[i : i + BQ_BATCH_SIZE]
        errors = client.insert_rows_json(table_fqdn, chunk)
        if errors:
            raise RuntimeError(f"BigQuery insert errors (sample): {errors[:3]}")


def main() -> None:
    bq = bigquery.Client(project=PROJECT_ID)
    table_fqdn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    ensure_bq_table(bq, table_fqdn)

    seen_place_ids = set()
    total_calls = 0
    total_rows = 0

    grid_points = list(generate_grid_points(ROTTERDAM_BBOX, GRID_STEP_M))
    print(f"Grid points: {len(grid_points)} (radius={RADIUS_M}m, step={GRID_STEP_M}m)")

    for kw in KEYWORDS:
        for (tile_lat, tile_lng) in grid_points:
            places = fetch_nearby_search_all_pages(tile_lat, tile_lng, kw)
            total_calls += 1  # counts tile calls (pagination adds more but same tile)

            # Deduplicate across tiles/keywords by place_id (optional but very helpful)
            unique_places = []
            for p in places:
                pid = p.get("place_id")
                if pid and pid not in seen_place_ids:
                    seen_place_ids.add(pid)
                    unique_places.append(p)

            rows = to_bq_rows(CITY_LABEL, kw, tile_lat, tile_lng, unique_places)
            if rows:
                insert_rows_batched(bq, table_fqdn, rows)
                total_rows += len(rows)

            if total_calls % 20 == 0:
                print(f"[Progress] calls={total_calls}, inserted_rows={total_rows}, unique_places={len(seen_place_ids)}")

            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    print(f"Done. calls={total_calls}, inserted_rows={total_rows}, unique_places={len(seen_place_ids)}")


if __name__ == "__main__":
    main()
