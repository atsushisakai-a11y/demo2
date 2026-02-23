{{ config(materialized='table', schema='staging', alias='staging_test_google_poi') }}

SELECT
  raw.place_id,
  raw.name,
  raw.address,
  raw.lat,
  raw.lng,
  IFNULL(raw.rating, 0) AS rating,
  IFNULL(raw.user_ratings_total, 0) AS user_ratings_total,
  raw.google_maps_url,
  CAST(DATE_TRUNC(raw.fetched_at, DAY) AS DATE) AS fetched_at
FROM `eneco-488308.raw.raw_test_google_poi` AS raw
