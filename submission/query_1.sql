-- query to create an actors table
-- column quality_class: A categorical bucketing of the average rating of the movies for this actor in their most recent year
CREATE OR REPLACE TABLE RaviT.actors(
  actor VARCHAR,
  actor_id VARCHAR,
  films ARRAY(
    ROW(
      film VARCHAR,
      votes INTEGER,
      rating INTEGER,
      film_id VARCHAR
    )
  ), 
  quality_class VARCHAR,
  is_active BOOLEAN,
  current_year INTEGER
)
WITH
  (
    FORMAT = 'PARQUET',
    partitioning = ARRAY['current_year']
  )