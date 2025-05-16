SELECT
  faa,
  DATE_PART('week', date) AS week,
  DATE_PART('year', date) AS year,
  MIN(temp_min) AS week_min_temp,
  MAX(temp_max) AS week_max_temp,
  AVG(temp_avg) AS week_avg_temp,
  SUM(precipitation) AS week_total_precipitation,
  SUM(snow) AS week_total_snow,
  AVG(wind_speed) AS week_avg_wind_speed,
  MAX(wind_gust) AS week_peak_gust
FROM {{ ref('prep_weather_daily') }}
GROUP BY faa, DATE_PART('week', date), DATE_PART('year', date)
