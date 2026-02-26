{{ config(
    materialized='table',
    schema='datamart',
    alias='datamart_smb_opportunity_score'
) }}

SELECT
    d.place_id,
    d.name,
    d.primary_type,
    d.address,
    d.lat,
    d.lng,
    d.google_maps_url,

    f.rating,
    f.user_ratings_total,
    f.opportunity_score,
    f.priority_tier,
    f.fetched_at

FROM {{ ref('dim_google_poi') }} AS d
LEFT JOIN {{ ref('fact_smb_opportunity_score') }} AS f
    ON d.place_id = f.place_id

WHERE f.opportunity_score IS NOT NULL
