{{ config(
    materialized='table',
    schema='dwh',
    alias='fact_smb_opportunity_score'
) }}

WITH latest AS (

    SELECT
        s.place_id,
        s.name,
        s.address,
        s.primary_type,
        s.lat,
        s.lng,
        s.google_maps_url,
        s.rating,
        s.user_ratings_total,
        s.fetched_at,

        ROW_NUMBER() OVER (
            PARTITION BY s.place_id
            ORDER BY s.fetched_at DESC
        ) AS rn

    FROM {{ ref('staging_google_poi') }} AS s

),

scored AS (

    SELECT
        place_id,
        name,
        primary_type,
        address,
        lat,
        lng,
        google_maps_url,
        rating,
        user_ratings_total,
        fetched_at,

        -- 📊 Google signal proxy (business popularity)
        SAFE_MULTIPLY(rating, LN(1 + user_ratings_total)) AS business_strength,

        -- ⚡ Energy intensity weighting (industry-based)
        CASE
            -- 🥇 High energy intensity (food + hospitality)
            WHEN primary_type IN (
                'restaurant', 'meal_takeaway', 'meal_delivery',
                'cafe', 'bar',
                'supermarket', 'grocery_or_supermarket',
                'convenience_store', 'liquor_store',
                'lodging'
            ) THEN 1.7

            -- 🥈 Medium-high
            WHEN primary_type IN (
                'bakery', 'drugstore', 'florist',
                'pet_store', 'furniture_store',
                'department_store', 'shopping_mall'
            ) THEN 1.4

            -- 🥉 Medium retail & service
            WHEN primary_type IN (
                'beauty_salon', 'hair_care', 'spa',
                'clothing_store', 'shoe_store',
                'electronics_store', 'home_goods_store',
                'jewelry_store', 'bicycle_store'
            ) THEN 1.2

            -- ⚡ Energy-related footprint
            WHEN primary_type = 'gas_station' THEN 1.5

            -- ⚖️ Office / professional
            WHEN primary_type IN (
                'doctor', 'health',
                'finance', 'travel_agency',
                'general_contractor', 'car_dealer'
            ) THEN 1.0

            -- 🧊 Low direct energy
            WHEN primary_type IN (
                'atm', 'point_of_interest',
                'museum', 'tourist_attraction'
            ) THEN 0.9

            ELSE 1.0
        END AS energy_weight

    FROM latest
    WHERE rn = 1

),

final_scored AS (

    SELECT
        *,
        business_strength * energy_weight AS opportunity_score
    FROM scored

),

bucketed AS (

    SELECT
        *,
        NTILE(10) OVER (ORDER BY opportunity_score DESC) AS decile
    FROM final_scored

)

SELECT
    place_id,
    name,
    primary_type,
    address,
    lat,
    lng,
    google_maps_url,
    fetched_at,
    rating,
    user_ratings_total,
    opportunity_score,

    CASE
        WHEN decile IN (1,2) THEN 'Gold'
        WHEN decile IN (3,4,5) THEN 'Silver'
        ELSE 'Bronze'
    END AS priority_tier

FROM bucketed
ORDER BY opportunity_score DESC
