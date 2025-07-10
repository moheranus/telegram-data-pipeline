{{ config(
    materialized='table',
    schema='marts'
) }}

SELECT
    m.message_id,
    MD5(m.channel_name) AS channel_id,
    TO_CHAR(CAST(m.message_date AS DATE), 'YYYYMMDD')::INT AS date_id,
    m.text,
    m.has_image,
    m.sender_id,
    m.image_path,
    LENGTH(COALESCE(m.text, '')) AS message_length,
    m.scraped_date
FROM staging.stg_telegram_messages m
