WITH route_stats AS (
    SELECT 
        origin,
        dest,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT tail_number) AS unique_planes,
        COUNT(DISTINCT airline) AS unique_airlines,
        AVG(actual_elapsed_time) AS avg_elapsed_time,
        AVG(arr_delay) AS avg_arrival_delay,
        MAX(arr_delay) AS max_arrival_delay,
        MIN(arr_delay) AS min_arrival_delay,
        COUNT(CASE WHEN cancelled = 1 THEN 1 END) AS total_cancellations,
        COUNT(CASE WHEN diverted = 1 THEN 1 END) AS total_diversions
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, dest
)

SELECT 
    r.origin,
    o.name AS origin_airport_name,
    o.city AS origin_city,
    o.country AS origin_country,
    r.dest,
    d.name AS dest_airport_name,
    d.city AS dest_city,
    d.country AS dest_country,
    r.total_flights,
    r.unique_planes,
    r.unique_airlines,
    ROUND(r.avg_elapsed_time, 2) AS avg_elapsed_time,
    ROUND(r.avg_arrival_delay, 2) AS avg_arrival_delay,
    r.max_arrival_delay,
    r.min_arrival_delay,
    r.total_cancellations,
    r.total_diversions
FROM route_stats r
LEFT JOIN {{ ref('staging_airports') }} o
    ON r.origin = o.faa
LEFT JOIN {{ ref('staging_airports') }} d
    ON r.dest = d.faa
ORDER BY r.total_flights DESC 