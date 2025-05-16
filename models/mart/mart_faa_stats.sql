WITH departure_connections AS (
    SELECT 
        origin AS faa,
        COUNT(DISTINCT flight_number) AS unique_departure_connections,
        COUNT(*) AS total_departures_planned,
        COUNT(DISTINCT tail_number) AS unique_planes_departures,
        COUNT(DISTINCT airline) AS unique_airlines_departures
    FROM {{ ref('prep_flights') }}
    GROUP BY origin
),

arrival_connections AS (
    SELECT 
        dest AS faa,
        COUNT(DISTINCT flight_number) AS unique_arrival_connections,
        COUNT(*) AS total_arrivals_planned,
        COUNT(DISTINCT tail_number) AS unique_planes_arrivals,
        COUNT(DISTINCT airline) AS unique_airlines_arrivals
    FROM {{ ref('prep_flights') }}
    GROUP BY dest
)

SELECT 
    a.faa,
    a.name AS airport_name,
    a.city,
    a.country,
    COALESCE(d.unique_departure_connections, 0) AS unique_departure_connections,
    COALESCE(ar.unique_arrival_connections, 0) AS unique_arrival_connections,
    COALESCE(d.total_departures_planned, 0) AS total_departures_planned,
    COALESCE(ar.total_arrivals_planned, 0) AS total_arrivals_planned,
    (COALESCE(d.total_departures_planned, 0) + COALESCE(ar.total_arrivals_planned, 0)) AS total_flights_planned,
    ROUND((COALESCE(d.unique_planes_departures, 0) + COALESCE(ar.unique_planes_arrivals, 0)) / 2.0, 2) AS avg_unique_planes,
    ROUND((COALESCE(d.unique_airlines_departures, 0) + COALESCE(ar.unique_airlines_arrivals, 0)) / 2.0, 2) AS avg_unique_airlines
FROM {{ ref('staging_airports') }} a
LEFT JOIN departure_connections d
    ON a.faa = d.faa
LEFT JOIN arrival_connections ar
    ON a.faa = ar.faa
ORDER BY total_flights_planned DESC