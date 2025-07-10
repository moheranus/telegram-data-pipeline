{{ config(
    materialized='table',
    schema='marts'
) }}

-- Create fact table for Telegram messages, joining with dimensions
SELECT
    m.message_id,  -- Unique message identifier
    c.channel_id,  -- Foreign key to dim_channels
    TO_CHAR(CAST(m.message_date AS DATE), 'YYYYMMDD')::INT AS date_id,  -- Foreign key to dim_dates
    m.text,  -- Message content
    m.has_image,  -- Boolean for image presence
    m.sender_id,  -- Sender ID
    m.image_path,  -- Path to image file
    LENGTH(COALESCE(m.text, '')) AS message_length,  -- Length of text (0 if empty)
    m.scraped_date  -- Date of data collection
FROM staging.stg_telegram_messages m
LEFT JOIN marts.dim_channels c ON m.channel_name = c.channel_name  -- Join with dim_channels
WHERE
    c.channel_id IS NOT NULL  -- Ensure valid channel_id