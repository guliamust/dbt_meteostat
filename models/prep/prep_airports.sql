WITH airports_reorder AS (
    SELECT
        faa,           -- FAA airport code
        name,          -- Airport name
        city,          -- City
        country,       -- Country
        alt,           -- Altitude
        lat,           -- Latitude
        lon,           -- Longitude
        tz            -- Timezone
    FROM {{ ref('staging_airports') }}
)
SELECT * FROM airports_reorder