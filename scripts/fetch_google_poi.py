import os
import time
import json
import math
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Iterable, Tuple, Optional

import requests
from requests.exceptions import HTTPError, Timeout, ConnectionError
from google.cloud import bigquery

# =========================
# Parameters
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

# Rotterdam bounding box (approx)
ROTTERDAM_BBOX = {"min_lat": 51.87, "max_lat": 52.02, "min_lng": 4.35, "max_lng": 4.60}
RADIUS_M = 2000
GRID_STEP_M = int(RADIUS_M * 0.8)
MAX_PAGES_PER_TILE = 3

# Cost control (set high when you want full Rotterdam)
MAX_API_CALLS_TOTAL = 10
api_call_counter = 0

# BigQuery inserts
BQ_BATCH_SIZE = 500

# Throttling / retry
SLEEP_BETWEEN_CALLS_SEC = 0.2
SLEEP_FOR_NEXT_PAGE_TOKEN_SEC = 2.0
MAX_HTTP_RETRIES = 3
BACKOFF_BASE_SEC = 1.0

API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing GOOGLE_MAPS_API_KEY env var.")

NEARBYSEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_row_id(location: str, keyword: str, place_id: str) -> str:
    raw = f"{location}||{keyword}||{place_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def build_google_maps_url(place_id: str) -> str:
    # Stable deep link for a place_id
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


def meters_to_lat_deg(m: float) -> float:
    return m / 111_320.0


def meters_to_lng_deg(m: float, lat_deg: float) -> float:
    return m / (111_320.0 * math.cos(math.radians(lat_deg)))


def generate_grid_points(bbox: Dict[str, float], step_m: int) -> Iterable[Tuple[float, float]]:
    min_lat, max_lat = bbox["min_lat"], bbox["max_lat"]
    min_lng, max_lng = bbox["min_lng"], bbox["max_lng"]
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


def _safe_params(params: Dict[str, Any]) -> Dict[str, Any]:
    p = dict(params)
    if "key" in p:
        p["key"] = "***"
    return p


def http_get_with_retry(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Counts every real HTTP request towards MAX_API_CALLS_TOTAL.
    Retries transient errors (500/502/503/504) + network timeouts.
    """
    global api_call_counter

    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        if api_call_counter >= MAX_API_CALLS_TOTAL:
            print("[STOP] API limit reached before request.")
            return None

        try:
            resp = requests.get(url, params=params, timeout=30)

            api_call_counter += 1
            print(f"[API CALL] {api_call_counter}/{MAX_API_CALLS_TOTAL} params={_safe_params(params)}")

            if resp.status_code in (500, 502, 503, 504):
                raise HTTPError(f"HTTP {resp.status_code}", response=resp)

            resp.raise_for_status()
            return resp.json()

        except (HTTPError, Timeout, ConnectionError) as e:
            wait = min(BACKOFF_BASE_SEC * (2 ** (attempt - 1)), 10)
            print(f"[WARN] HTTP retry {attempt}/{MAX_HTTP_RETRIES} err={e}. sleep={wait}s")
            time.sleep(wait)

    print(f"[ERROR] Giving up request after retries. params={_safe_params(params)}")
    return None


def fetch_nearby_all_pages(lat: float, lng: float, keyword: str) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []
    next_token = None

    for page in range(MAX_PAGES_PER_TILE):
        if api_call_counter >= MAX_API_CALLS_TOTAL:
            print("[STOP] API limit reached during pagination.")
            break

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

        payload = http_get_with_retry(NEARBYSEARCH_URL, params)
        if payload is None:
            break

        status = payload.get("status")
        if status == "OVER_QUERY_LIMIT":
            print("[WARN] OVER_QUERY_LIMIT. Sleep 10s and try once more.")
            time.sleep(10)
            payload = http_get_with_retry(NEARBYSEARCH_URL, params)
            if payload is None:
                break
            status = payload.get("status")

        if status not in ("OK", "ZERO_RESULTS"):
            print(f"[WARN] Places status={status} (tile={lat},{lng} keyword={keyword})")
            break

        results = payload.get("results", [])
        all_results.extend(results)
        next_token = payload.get("next_page_token")

        print(
            f"[FETCH] tile=({lat},{lng}) keyword='{keyword}' page={page+1} "
            f"status={status} results={len(results)} total_tile_results={len(all_results)}"
        )

        if not next_token:
            break

    return all_results


def to_bq_rows(location: str, keyword: str, places: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    fetched_at = now_utc_iso()
    rows: List[Dict[str, Any]] = []

    for p in places:
        place_id = p.get("place_id")
        if not place_id:
            continue

        geom = (p.get("geometry") or {}).get("location") or {}

        rows.append(
            {
                "fetched_at": fetched_at,
                "location": location,
                "keyword": keyword,
                "row_id": stable_row_id(location, keyword, place_id),

                "place_id": place_id,
                "google_maps_url": build_google_maps_url(place_id),

                "name": p.get("name"),
                "formatted_address": p.get("vicinity"),
                "business_status": p.get("business_status"),
                "types": p.get("types", []) or [],
                "rating": p.get("rating"),
                "user_ratings_total": p.get("user_ratings_total"),
                "price_level": p.get("price_level"),
                "lat": geom.get("lat"),
                "lng": geom.get("lng"),

                "raw_json": json.dumps(p, ensure_ascii=False),
            }
        )

    return rows


def insert_rows_batched_with_logs(
    client: bigquery.Client,
    table_fqdn: str,
    rows: List[Dict[str, Any]],
    context: str,
) -> int:
    """
    Returns number of rows successfully inserted (best-effort).
    Logs explicitly: attempted, inserted, errors.
    """
    if not rows:
        print(f"[BQI] {context} attempted=0 (skip)")
        return 0

    inserted = 0
    for i in range(0, len(rows), BQ_BATCH_SIZE):
        chunk = rows[i : i + BQ_BATCH_SIZE]
        print(f"[BQI] {context} table={table_fqdn} attempt_insert={len(chunk)} batch={i//BQ_BATCH_SIZE+1}")

        errors = client.insert_rows_json(table_fqdn, chunk)

        if errors:
            error_count = len(errors)
            ok_count = len(chunk) - error_count
            inserted += max(ok_count, 0)
            print(f"[BQI] {context} batch_result inserted~={ok_count} errors={error_count} sample_errors={errors[:2]}")
        else:
            inserted += len(chunk)
            print(f"[BQI] {context} batch_result inserted={len(chunk)} errors=0")

    return inserted


def main() -> None:
    bq = bigquery.Client(project=PROJECT_ID)
    table_fqdn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    print(f"[START] target_table={table_fqdn} location='{LOCATION}' max_api_calls={MAX_API_CALLS_TOTAL}")

    grid = list(generate_grid_points(ROTTERDAM_BBOX, GRID_STEP_M))
    print(f"Grid points: {len(grid)} (radius={RADIUS_M}m, step={GRID_STEP_M}m)")

    seen_place_ids = set()
    total_found_unique = 0
    total_inserted = 0
    tile_idx = 0

    for kw in KEYWORDS:
        for (tile_lat, tile_lng) in grid:
            if api_call_counter >= MAX_API_CALLS_TOTAL:
                print("[STOP] API limit reached. Ending run.")
                print(f"[SUMMARY] api_calls={api_call_counter} found_unique={total_found_unique} inserted={total_inserted} table={table_fqdn}")
                return

            tile_idx += 1
            places = fetch_nearby_all_pages(tile_lat, tile_lng, kw)

            unique = []
            for p in places:
                pid = p.get("place_id")
                if pid and pid not in seen_place_ids:
                    seen_place_ids.add(pid)
                    unique.append(p)

            total_found_unique += len(unique)
            rows = to_bq_rows(LOCATION, kw, unique)

            context = f"kw='{kw}' tile#{tile_idx} ({tile_lat},{tile_lng}) unique={len(unique)}"
            inserted_now = insert_rows_batched_with_logs(bq, table_fqdn, rows, context=context)
            total_inserted += inserted_now

            print(f"[PROGRESS] {context} inserted_now={inserted_now} total_inserted={total_inserted} api_calls={api_call_counter}")

            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    print(f"[DONE] api_calls={api_call_counter} found_unique={total_found_unique} inserted={total_inserted} table={table_fqdn}")


if __name__ == "__main__":
    main()
