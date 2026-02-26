{{ config(
    materialized='table',
    schema='dwh',
    alias='dim_google_poi'
) }}

WITH deduplicated AS (

    SELECT
        s.place_id,
        s.name,
        s.address,
        s.primary_type,
        s.lat,
        s.lng,
        s.google_maps_url,
        ROW_NUMBER() OVER (
            PARTITION BY s.place_id
            ORDER BY s.fetched_at DESC
        ) AS rn

    FROM {{ ref('staging_google_poi') }} AS s

)

SELECT
    place_id,
    name,
    address,
    primary_type,
    lat,
    lng,
    google_maps_url

FROM deduplicated
WHERE rn = 1
