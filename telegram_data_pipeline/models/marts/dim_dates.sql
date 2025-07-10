{{ config(
    materialized='table',
    schema='marts'
) }}

WITH date_range AS (
    SELECT generate_series(
        '2025-01-01'::date,
        '2025-12-31'::date,
        interval '1 day'
    ) AS date_day
)
SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_id,
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Day') AS day_name
FROM date_range
