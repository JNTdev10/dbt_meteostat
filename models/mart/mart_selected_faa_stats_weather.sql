WITH airport_staids AS (
    SELECT 'JFK' AS faa, 74486 AS station_id UNION ALL
    SELECT 'MIA', 72202 UNION ALL
    SELECT 'LAX', 72295
),
airport_details AS (
    SELECT
        a.faa,
        a.name AS airport_name,
        a.city,
        a.country,
        s.station_id
    FROM {{ ref('staging_airports') }} a
    INNER JOIN airport_staids s ON a.faa = s.faa
),
flight_stats AS (
    SELECT
        ad.faa,
        f.flight_date,
        COUNT(DISTINCT CASE WHEN f.origin = ad.faa THEN f.dest END) AS unique_departure_connections,
        COUNT(DISTINCT CASE WHEN f.dest = ad.faa THEN f.origin END) AS unique_arrival_connections,
        COUNT(CASE WHEN f.origin = ad.faa THEN 1 END) AS total_departures,
        COUNT(CASE WHEN f.dest = ad.faa THEN 1 END) AS total_arrivals,
        COUNT(CASE WHEN f.origin = ad.faa AND f.cancelled = 1 THEN 1 END) AS cancelled_departures,
        COUNT(CASE WHEN f.dest = ad.faa AND f.cancelled = 1 THEN 1 END) AS cancelled_arrivals,
        COUNT(CASE WHEN f.origin = ad.faa AND f.diverted = 1 THEN 1 END) AS diverted_departures,
        COUNT(CASE WHEN f.dest = ad.faa AND f.diverted = 1 THEN 1 END) AS diverted_arrivals,
        COUNT(CASE WHEN f.origin = ad.faa AND f.cancelled = 0 AND f.diverted = 0 THEN 1 END) AS actual_departures,
        COUNT(CASE WHEN f.dest = ad.faa AND f.cancelled = 0 AND f.diverted = 0 THEN 1 END) AS actual_arrivals,
        COUNT(DISTINCT CASE WHEN f.origin = ad.faa THEN f.tail_number END) AS unique_planes_departure,
        COUNT(DISTINCT CASE WHEN f.dest = ad.faa THEN f.tail_number END) AS unique_planes_arrival,
        COUNT(DISTINCT CASE WHEN f.origin = ad.faa THEN f.airline END) AS unique_airlines_departure,
        COUNT(DISTINCT CASE WHEN f.dest = ad.faa THEN f.airline END) AS unique_airlines_arrival
    FROM airport_details ad
    LEFT JOIN {{ ref('prep_flights') }} f
        ON ad.faa IN (f.origin, f.dest)
    GROUP BY ad.faa, f.flight_date
),
weather_stats AS (
    SELECT
        s.faa,
        w.date AS flight_date,
        w.min_temp_c AS daily_min_temperature,
        w.max_temp_c AS daily_max_temperature,
        w.precipitation_mm AS daily_precipitation,
        w.max_snow_mm AS daily_snow_fall,
        w.avg_wind_direction AS daily_avg_wind_direction,
        w.avg_wind_speed_kmh AS daily_avg_wind_speed,
        w.wind_peakgust_kmh AS daily_wind_peakgust
    FROM airport_staids s
    JOIN {{ ref('prep_weather_daily') }} w
        ON s.station_id = w.station_id
)
SELECT
    fd.faa,
    ad.airport_name,
    ad.city,
    ad.country,
    fd.flight_date,
    fd.unique_departure_connections,
    fd.unique_arrival_connections,
    fd.total_departures + fd.total_arrivals AS total_flights_planned,
    fd.cancelled_departures + fd.cancelled_arrivals AS total_flights_cancelled,
    fd.diverted_departures + fd.diverted_arrivals AS total_flights_diverted,
    fd.actual_departures + fd.actual_arrivals AS total_flights_actual,
    ROUND((fd.unique_planes_departure + fd.unique_planes_arrival) / 2.0, 2) AS avg_unique_planes,
    ROUND((fd.unique_airlines_departure + fd.unique_airlines_arrival) / 2.0, 2) AS avg_unique_airlines,
    ws.daily_min_temperature,
    ws.daily_max_temperature,
    ws.daily_precipitation,
    ws.daily_snow_fall,
    ws.daily_avg_wind_direction,
    ws.daily_avg_wind_speed,
    ws.daily_wind_peakgust
FROM flight_stats fd
JOIN airport_details ad ON fd.faa = ad.faa
LEFT JOIN weather_stats ws ON fd.faa = ws.faa AND fd.flight_date = ws.flight_date
ORDER BY fd.faa, fd.flight_date 