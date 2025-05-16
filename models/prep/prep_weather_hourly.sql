WITH hourly_data AS (
    SELECT * 
    FROM {{ ref('staging_weather_hourly') }}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date                                 -- only date
        , timestamp::TIME AS time                                 -- only time
        , TO_CHAR(timestamp, 'HH24:MI') AS hour                   -- time as text
        , TO_CHAR(timestamp, 'FMmonth') AS month_name             -- month name as text
        , TO_CHAR(timestamp, 'Day') AS weekday                    -- weekday name as text
        , DATE_PART('day', timestamp) AS date_day                 -- day of month
        , DATE_PART('month', timestamp) AS date_month             -- month number
        , DATE_PART('year', timestamp) AS date_year               -- year
        , DATE_PART('week', timestamp) AS cw                      -- calendar week
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        , CASE 
            WHEN time BETWEEN TIME '00:00:00' AND TIME '05:59:59' THEN 'night'
            WHEN time BETWEEN TIME '06:00:00' AND TIME '11:59:59' THEN 'morning'
            WHEN time BETWEEN TIME '12:00:00' AND TIME '17:59:59' THEN 'day'
            WHEN time BETWEEN TIME '18:00:00' AND TIME '23:59:59' THEN 'evening'
            ELSE 'unknown'
        END AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features