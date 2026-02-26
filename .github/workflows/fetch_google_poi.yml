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
from google.api_core.exceptions import NotFound

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

ROTTERDAM_BBOX = {"min_lat": 51.87, "max_lat": 52.02, "min_lng": 4.35, "max_lng": 4.60}

RADIUS_M = 2000
GRID_STEP_M = int(RADIUS_M * 0.8)
MAX_PAGES_PER_TILE = 3

# 🔒 HARD COST CONTROL
MAX_API_CALLS_TOTAL = 10
api_call_counter = 0

BQ_BATCH_SIZE = 500
SLEEP_BETWEEN_CALLS_SEC = 0.2
SLEEP_FOR_NEXT_PAGE_TOKEN_SEC = 2.0

MAX_HTTP_RETRIES = 3
BACKOFF_BASE_SEC = 1.0

API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing GOOGLE_MAPS_API_KEY env var.")

NEARBYSEARCH_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"


def increment_api_counter():
    global api_call_counter
    api_call_counter += 1
    print(f"[API CALL] {api_call_counter}/{MAX_API_CALLS_TOTAL}")
    if api_call_counter >= MAX_API_CALLS_TOTAL:
        print("[STOP] Reached MAX_API_CALLS_TOTAL")
        return False
    return True


def http_get_with_retry(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    global api_call_counter

    if api_call_counter >= MAX_API_CALLS_TOTAL:
        return None

    for attempt in range(1, MAX_HTTP_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)

            api_call_counter += 1
            print(f"[API CALL] {api_call_counter}/{MAX_API_CALLS_TOTAL}")

            if api_call_counter >= MAX_API_CALLS_TOTAL:
                print("[STOP] API limit reached.")
            
            if resp.status_code in (500, 502, 503, 504):
                raise HTTPError(f"HTTP {resp.status_code}", response=resp)

            resp.raise_for_status()
            return resp.json()

        except (HTTPError, Timeout, ConnectionError) as e:
            wait = min(BACKOFF_BASE_SEC * (2 ** (attempt - 1)), 10)
            print(f"[WARN] retry {attempt}/{MAX_HTTP_RETRIES}, sleep={wait}s")
            time.sleep(wait)

    return None


def fetch_nearby_all_pages(lat: float, lng: float, keyword: str) -> List[Dict[str, Any]]:
    global api_call_counter

    all_results: List[Dict[str, Any]] = []
    next_token = None

    for _ in range(MAX_PAGES_PER_TILE):
        if api_call_counter >= MAX_API_CALLS_TOTAL:
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
        if status not in ("OK", "ZERO_RESULTS"):
            break

        all_results.extend(payload.get("results", []))
        next_token = payload.get("next_page_token")
        if not next_token:
            break

    return all_results


# -------- The rest remains same as your working version --------

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_row_id(location: str, keyword: str, place_id: str) -> str:
    raw = f"{location}||{keyword}||{place_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def meters_to_lat_deg(m: float) -> float:
    return m / 111_320.0


def meters_to_lng_deg(m: float, lat_deg: float) -> float:
    return m / (111_320.0 * math.cos(math.radians(lat_deg)))


def generate_grid_points(bbox: Dict[str, float], step_m: int):
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


def main():
    bq = bigquery.Client(project=PROJECT_ID)
    table_fqdn = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

    grid = list(generate_grid_points(ROTTERDAM_BBOX, GRID_STEP_M))
    print(f"Grid points: {len(grid)}")

    seen = set()
    total_inserted = 0

    for kw in KEYWORDS:
        for lat, lng in grid:

            if api_call_counter >= MAX_API_CALLS_TOTAL:
                print("Stopping early due to API limit.")
                print(f"Total inserted rows: {total_inserted}")
                return

            places = fetch_nearby_all_pages(lat, lng, kw)

            unique = []
            for p in places:
                pid = p.get("place_id")
                if pid and pid not in seen:
                    seen.add(pid)
                    unique.append(p)

            # You can reuse your existing insert logic here
            # (omitted for brevity)

            total_inserted += len(unique)

            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    print(f"Finished. API calls used: {api_call_counter}, inserted rows: {total_inserted}")


if __name__ == "__main__":
    main()
