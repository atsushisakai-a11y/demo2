{{ config(materialized='view') }}

select *
from {{ source('raw', 'raw_google_poi') }}
