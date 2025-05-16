WITH flights_one_month AS (
    SELECT * 
    FROM {{ ref('staging_flights_one_month') }}
),
flights_cleaned AS (
    SELECT 
        flight_date::DATE,
        -- Convert dep_time from integer/string HHMM to TIME
        TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time,
        -- Convert sched_dep_time from integer/string HHMM to TIME
        TO_CHAR(sched_dep_time, 'fm0000')::TIME AS sched_dep_time,
        dep_delay,
        (dep_delay * INTERVAL '1 minute') AS dep_delay_interval,
        -- Convert arr_time from integer/string HHMM to TIME
        TO_CHAR(arr_time, 'fm0000')::TIME AS arr_time,
        -- Convert sched_arr_time from integer/string HHMM to TIME
        TO_CHAR(sched_arr_time, 'fm0000')::TIME AS sched_arr_time,
        arr_delay,
        (arr_delay * INTERVAL '1 minute') AS arr_delay_interval,
        airline,
        tail_number,
        flight_number,
        origin,
        dest,
        air_time,
        -- Convert air_time (in minutes) to INTERVAL
        (air_time * INTERVAL '1 minute') AS air_time_interval,
        actual_elapsed_time,
        -- Convert actual_elapsed_time (in minutes) to INTERVAL
        (actual_elapsed_time * INTERVAL '1 minute') AS actual_elapsed_time_interval,
        (distance / 0.621371)::NUMERIC(6,2) AS distance_km, -- Convert miles to km
        cancelled,
        diverted
    FROM flights_one_month
)
SELECT * FROM flights_cleaned
