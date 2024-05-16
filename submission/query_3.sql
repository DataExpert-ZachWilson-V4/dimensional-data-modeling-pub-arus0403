-- Query to create Type 2 Slowly Changing Dimension Table tracking  quality_class and is_active columns
CREATE OR REPLACE TABLE RaviT.actors_history_scd  (
  actor_id VARCHAR,
  quality_class VARCHAR,
  is_active BOOLEAN,
  start_date INTEGER,
  end_date INTEGER,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )