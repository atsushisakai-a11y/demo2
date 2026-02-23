{{ config(materialized='table', schema='dwh', alias='dim_test_google_poi') }}

SELECT
  s.place_id,
  s.name,
  s.address,
  s.lat,
  s.lng,
  s.google_maps_url
FROM `eneco-488308.staging.staging_test_google_poi` AS s
ORDER BY 1, 2, 3
