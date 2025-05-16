WITH flights AS (
  SELECT * FROM {{ ref('prep_flights') }}
)
SELECT
  f.origin,
  f.dest,
  ao.city AS origin_city,
  ao.country AS origin_country,
  ao.name AS origin_name,
  ad.city AS dest_city,
  ad.country AS dest_country,
  ad.name AS dest_name,
  COUNT(*) AS total_flights,
  COUNT(DISTINCT f.tail_number) AS unique_airplanes,
  COUNT(DISTINCT f.airline) AS unique_airlines,
  AVG(f.actual_elapsed_time) AS avg_elapsed_time,
  AVG(f.arr_delay) AS avg_arr_delay,
  MAX(f.arr_delay) AS max_arr_delay,
  MIN(f.arr_delay) AS min_arr_delay,
  SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) AS total_cancelled,
  SUM(CASE WHEN f.diverted THEN 1 ELSE 0 END) AS total_diverted
FROM flights f
LEFT JOIN {{ ref('staging_airports') }} ao ON f.origin = ao.faa
LEFT JOIN {{ ref('staging_airports') }} ad ON f.dest = ad.faa
GROUP BY f.origin, f.dest, ao.city, ao.country, ao.name, ad.city, ad.country, ad.name