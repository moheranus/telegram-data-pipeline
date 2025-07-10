{{ config(
    materialized='table',
    schema='staging'
) }}

-- Stage raw Telegram messages, cleaning and standardizing data
SELECT
    message_id,  -- Unique message identifier from raw data
    COALESCE(channel_name, 'unknown') AS channel_name,  -- Replace null channel_name with 'unknown'
    CAST(COALESCE(message_date, '2020-01-01') AS TIMESTAMP) AS message_date,  -- Cast to TIMESTAMP, default to 2020-01-01 for nulls
    COALESCE(text, '') AS text,  -- Replace null text with empty string
    COALESCE(has_image, FALSE) AS has_image,  -- Default to FALSE if null
    sender_id,  -- Sender ID, nullable
    COALESCE(image_path, '') AS image_path,  -- Replace null image_path with empty string
    CAST(COALESCE(scraped_date, CURRENT_DATE) AS DATE) AS scraped_date  -- Cast to DATE, default to current date
FROM raw.telegram_messages
WHERE
    message_id IS NOT NULL  -- Ensure message_id is not null
    AND channel_name IS NOT NULL  -- Filter out rows with null channel_name
    AND message_date IS NOT NULL  -- Filter out rows with null message_date
    