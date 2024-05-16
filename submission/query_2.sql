 --Query  populates the actors table one year at a time starting from 2018 to 2021
INSERT INTO
  RaviT.actors
 WITH
  last_year AS (
    SELECT
      *
    FROM
      RaviT.actors
    WHERE
      current_year = 2020
  ),
  this_year AS (
    SELECT
      actor,
	  actor_id,
	  ARRAY_AGG(
      ROW(
        film,
        votes,
        rating,
        film_id)) 
  AS films,
  avg(rating) avg_rating,
  year  	  
    FROM
      bootcamp.actor_films 
	       where year = 2021
	group by actor,actor_id, year
  )
SELECT
  COALESCE(ly.actor, ty.actor) AS actor,
  COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
  CASE
    WHEN ty.year IS NULL THEN ly.films
    WHEN ty.year IS NOT NULL
    AND ly.films IS NULL THEN  ty.films
    WHEN ty.year IS NOT NULL
    AND ly.films IS NOT NULL THEN ty.films|| ly.films
  END AS films,
  case when ty.avg_rating is null  then ly.quality_class
  else
	case when ty.avg_rating>8 then 'star' 
		 when ty.avg_rating>7 and  ty.avg_rating<=8 then 'good' 
		 when ty.avg_rating>6 and  ty.avg_rating<=7 then 'average' 
		 when ty.avg_rating<=6 then 'bad' 
    end
  end as quality_class,
  ty.year IS NOT NULL AS is_active,
  COALESCE(ty.year, ly.current_year + 1) AS current_year
FROM
  last_year ly
  FULL OUTER JOIN this_year ty ON ly.actor_id = ty.actor_id