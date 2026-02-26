{{ config(materialized='table', schema='datamart', alias='datamart_google_poi') }}

SELECT
  CAST(DATE_TRUNC(f.fetched_at, MONTH) AS DATE) AS fetched_at,
  COUNT(DISTINCT d.place_id) AS places,
  ROUND(AVG(f.rating), 2) AS rating,
  ROUND(AVG(f.user_ratings_total), 2) AS user_ratings_total
FROM {{ ref('dim_google_poi') }} AS d
INNER JOIN {{ ref('fact_google_poi') }} AS f
  ON f.place_id = d.place_id
GROUP BY 1
ORDER BY 1
