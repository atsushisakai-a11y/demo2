#!/usr/bin/env python3
"""
Load Google POI JSON (list[dict]) into BigQuery.

Expected input JSON keys (from your sample):
- place_id, name, address, lat, lng, types, rating, user_ratings_total,
  google_maps_url, search_keyword, search_radius_m, fetched_at

Env vars:
  GOOGLE_CLOUD_PROJECT   (required)
  BQ_DATASET             (required)
  BQ_TABLE               (required)
  INPUT_JSON             (optional; default: google_poi_json.json)
  WRITE_DISPOSITION      (optional; default: WRITE_APPEND; can be WRITE_TRUNCATE)
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from dateutil import parser as dtparser
from google.cloud import bigquery


def _get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or val.strip() == ""):
        raise ValueError(f"Missing required env var: {name}")
    return val  # type: ignore[return-value]


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if s == "" or s.lower() == "none":
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def _to_timestamp_iso(v: Any) -> Optional[str]:
    """
    BigQuery NDJSON timestamp: use RFC3339 / ISO8601.
    Your data example: '2025-11-07 19:32:48.907788 UTC'
    """
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "none":
        return None

    # dateutil can parse the "UTC" suffix; ensure timezone-aware
    dt = dtparser.parse(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def transform_record(r: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "place_id": r.get("place_id"),
        "name": r.get("name"),
        "address": r.get("address"),
        "lat": _to_float(r.get("lat")),
        "lng": _to_float(r.get("lng")),
        # Keep as a single comma-separated STRING, as in your file.
        # (If you prefer ARRAY<STRING>, we can split and load as repeated field.)
        "types": r.get("types"),
        "rating": _to_float(r.get("rating")),
        "user_ratings_total": _to_int(r.get("user_ratings_total")),
        "google_maps_url": r.get("google_maps_url"),
        "search_keyword": r.get("search_keyword"),
        "search_radius_m": _to_int(r.get("search_radius_m")),
        "fetched_at": _to_timestamp_iso(r.get("fetched_at")),
        # Optional ingestion timestamp (useful for debugging / freshness)
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def write_ndjson(rows: Iterable[Dict[str, Any]], out_path: str) -> int:
    n = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
    return n


def main() -> int:
    project = _get_env("GOOGLE_CLOUD_PROJECT", required=True)
    dataset = _get_env("BQ_DATASET", required=True)
    table = _get_env("BQ_TABLE", required=True)
    input_json = _get_env("INPUT_JSON", default="google_poi_json.json")
    write_disposition = _get_env("WRITE_DISPOSITION", default="WRITE_APPEND")

    table_id = f"{project}.{dataset}.{table}"

    if not os.path.exists(input_json):
        print(f"ERROR: INPUT_JSON not found at: {input_json}", file=sys.stderr)
        return 2

    with open(input_json, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, list):
        print("ERROR: Expected the JSON file to contain a top-level list[]", file=sys.stderr)
        return 3

    transformed = (transform_record(r) for r in payload if isinstance(r, dict))

    with tempfile.TemporaryDirectory() as tmpdir:
        ndjson_path = os.path.join(tmpdir, "poi.ndjson")
        row_count = write_ndjson(transformed, ndjson_path)
        print(f"Prepared {row_count} rows -> {ndjson_path}")

        client = bigquery.Client(project=project)

        schema = [
            bigquery.SchemaField("place_id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("lat", "FLOAT"),
            bigquery.SchemaField("lng", "FLOAT"),
            bigquery.SchemaField("types", "STRING"),
            bigquery.SchemaField("rating", "FLOAT"),
            bigquery.SchemaField("user_ratings_total", "INT64"),
            bigquery.SchemaField("google_maps_url", "STRING"),
            bigquery.SchemaField("search_keyword", "STRING"),
            bigquery.SchemaField("search_radius_m", "INT64"),
            bigquery.SchemaField("fetched_at", "TIMESTAMP"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
            # Partition by fetched_at (good for analytics and incremental loads)
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="fetched_at",
            ),
        )

        with open(ndjson_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)

        print(f"BigQuery load job started: {job.job_id}")
        job.result()  # wait
        dest = client.get_table(table_id)
        print(f"Loaded into {table_id}. Table now has {dest.num_rows} rows.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
