{{ config(
    materialized='table',
    schema='marts'
) }}

-- Generate date dimension for 2020–2025 to cover all message dates
WITH date_range AS (
    SELECT generate_series(
        '2020-01-01'::date,
        '2025-12-31'::date,
        interval '1 day'
    ) AS date_day
)
SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_id,  -- Unique date_id in YYYYMMDD format
    date_day,  -- Full date
    EXTRACT(YEAR FROM date_day) AS year,  -- Year component
    EXTRACT(MONTH FROM date_day) AS month,  -- Month component
    EXTRACT(DAY FROM date_day) AS day,  -- Day component
    EXTRACT(DOW FROM date_day) AS day_of_week,  -- Day of week (0-6, Sunday-Saturday)
    TO_CHAR(date_day, 'Day') AS day_name  -- Day name (e.g., Monday)
FROM date_range
