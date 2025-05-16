WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
        ,timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
        ,timestamp::TIME AS time                           -- only time (hours:minutes:seconds) as TIME data type
        ,TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
        ,TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
        ,TO_CHAR(timestamp, 'FMDay') AS weekday        -- weekday name as TEXT        
        ,DATE_PART('day', timestamp)::INT AS date_day
        ,DATE_PART('month', timestamp)::INT AS date_month
        ,DATE_PART('year', timestamp)::INT AS date_year
        ,DATE_PART('week', timestamp)::INT AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        ,(CASE 
            WHEN time BETWEEN TIME '00:00:00' AND TIME '05:59:59' THEN 'night'
            WHEN time BETWEEN TIME '06:00:00' AND TIME '17:59:59' THEN 'day'
            ELSE 'evening'
        END) AS day_part
    FROM add_features
)

SELECT *
FROM add_more_features