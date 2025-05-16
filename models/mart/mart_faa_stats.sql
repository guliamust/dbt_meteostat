WITH flights AS (
  SELECT * FROM {{ ref('prep_flights') }}
)
SELECT
  a.faa,
  a.city,
  a.country,
  a.name,
  COUNT(DISTINCT f.dest) AS unique_departure_connections,
  COUNT(DISTINCT f.origin) AS unique_arrival_connections,
  COUNT(*) AS total_flights,
  SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) AS total_cancelled,
  SUM(CASE WHEN f.diverted THEN 1 ELSE 0 END) AS total_diverted,
  SUM(CASE WHEN NOT f.cancelled AND NOT f.diverted THEN 1 ELSE 0 END) AS total_actual,
  COUNT(DISTINCT f.tail_number) AS unique_airplanes,
  COUNT(DISTINCT f.airline) AS unique_airlines
FROM {{ ref('staging_airports') }} a
LEFT JOIN flights f ON a.faa = f.origin OR a.faa = f.dest
GROUP BY a.faa, a.city, a.country, a.name
ORDER BY a.faa