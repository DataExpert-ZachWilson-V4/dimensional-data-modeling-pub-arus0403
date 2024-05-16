-- below is ackfill query that  populates the entire actors_history_scd table in a single query till year 2020
INSERT into RaviT.actors_history_scd
WITH
  lagged AS (
     SELECT
      actor_id,
      is_active,
      -- below column is using lag function to get previous year is_active value
	  LAG(is_active, 1) OVER (
          PARTITION BY
            actor_id
          ORDER BY
            current_year
        )  as is_active_last_year,
	   -- below column is using lag function to get previous year quality_class value
  	   quality_class,		
	      LAG(quality_class, 1) OVER (
          PARTITION BY
            actor_id
          ORDER BY
            current_year
        )  as quality_class_last_year,	
      current_year
    FROM
      RaviT.actors
    WHERE
      current_year <= 2020
  ),
  streaked AS (
    SELECT
      *,
      SUM(
        CASE
          WHEN is_active <> is_active_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year 
      ) AS streak_is_active_identifier,
	        SUM(
        CASE
          WHEN quality_class <> quality_class_last_year THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          actor_id
        ORDER BY
          current_year 
      ) AS streak_quality_class_identifier
    FROM
      lagged
  )
SELECT
  actor_id,
  MAX(quality_class) AS quality_class,
  MAX(is_active) AS is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2020 AS current_year
FROM
  streaked
GROUP BY
  actor_id,
  streak_is_active_identifier,
  streak_quality_class_identifier
   
   
   
   
  
 
  
  
  