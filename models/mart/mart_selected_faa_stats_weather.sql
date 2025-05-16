WITH flights AS (
  SELECT * FROM {{ ref('prep_flights') }}
),
weather AS (
  SELECT * FROM {{ ref('prep_weather_daily') }}
)
SELECT
  f.origin AS faa,
  a.city,
  a.country,
  a.name,
  f.flight_date,
  COUNT(DISTINCT f.dest) AS unique_departure_connections,
  COUNT(DISTINCT f.origin) AS unique_arrival_connections,
  COUNT(*) AS total_flights,
  SUM(CASE WHEN f.cancelled THEN 1 ELSE 0 END) AS total_cancelled,
  SUM(CASE WHEN f.diverted THEN 1 ELSE 0 END) AS total_diverted,
  SUM(CASE WHEN NOT f.cancelled AND NOT f.diverted THEN 1 ELSE 0 END) AS total_actual,
  MIN(w.temp_min) AS daily_min_temp,
  MAX(w.temp_max) AS daily_max_temp,
  SUM(w.precipitation) AS daily_precipitation,
  SUM(w.snow) AS daily_snow,
  AVG(w.wind_dir) AS daily_avg_wind_dir,
  AVG(w.wind_speed) AS daily_avg_wind_speed,
  MAX(w.wind_gust) AS daily_peak_gust
FROM flights f
JOIN weather w ON f.origin = w.faa AND f.flight_date = w.date
JOIN {{ ref('staging_airports') }} a ON f.origin = a.faa
GROUP BY f.origin, a.city, a.country, a.name, f.flight_date
