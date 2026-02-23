{{ config(materialized='table', schema='dwh', alias='fact_test_google_poi') }}

SELECT
  s.place_id,
  s.fetched_at,
  s.rating,
  s.user_ratings_total
FROM {{ ref('staging_test_google_poi') }} AS s
ORDER BY 1, 2, 3
