{{ config(
    materialized='table',
    schema='marts'
) }}

-- Create fact table for Telegram messages, joining with dimensions
SELECT
    m.message_id,
    c.channel_id,
    d.date_id,
    m.text,
    m.has_image,
    m.sender_id,
    m.image_path,
    LENGTH(COALESCE(m.text, '')) AS message_length,
    m.scraped_date
FROM {{ ref('stg_telegram_messages') }} m
LEFT JOIN {{ ref('dim_channels') }} c ON m.channel_name = c.channel_name
LEFT JOIN {{ ref('dim_dates') }} d ON TO_CHAR(CAST(m.message_date AS DATE), 'YYYYMMDD')::INT = d.date_id
WHERE
    c.channel_id IS NOT NULL
    AND d.date_id IS NOT NULL