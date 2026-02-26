{{ config(materialized='table', schema='dwh', alias='dim_google_poi') }}

SELECT
  s.place_id,
  s.name,
  s.address,
  s.primary_type,
  s.lat,
  s.lng,
  s.google_maps_url
FROM `eneco-488308.staging.staging_google_poi` AS s
GROUP BY ALL
ORDER BY 1, 2, 3
