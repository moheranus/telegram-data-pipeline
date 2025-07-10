{{ config(
    materialized='table',
    schema='staging'
) }}

SELECT
    message_id,
    channel_name,
    CAST(message_date AS TIMESTAMP) AS message_date,
    COALESCE(text, '') AS text,
    has_image,
    sender_id,
    image_path,
    CAST(scraped_date AS DATE) AS scraped_date
FROM raw.telegram_messages
WHERE
    message_id IS NOT NULL
    AND channel_name IS NOT NULL
    AND scraped_date IS NOT NULL
