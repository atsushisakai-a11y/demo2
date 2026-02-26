CREATE TABLE `eneco-488308.raw.raw_google_poi` (
  fetched_at TIMESTAMP NOT NULL,
  location STRING NOT NULL,
  keyword STRING NOT NULL,
  row_id STRING NOT NULL,

  place_id STRING NOT NULL,
  name STRING,
  formatted_address STRING,
  business_status STRING,
  types ARRAY<STRING>,

  rating FLOAT64,
  user_ratings_total INT64,
  price_level INT64,

  lat FLOAT64,
  lng FLOAT64,

  raw_json STRING
);

ALTER TABLE `eneco-488308.raw.raw_google_poi`
ADD COLUMN google_maps_url STRING;
