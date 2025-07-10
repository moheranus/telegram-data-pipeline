{{ config(
    materialized='table',
    schema='marts'
) }}

-- Create dimension table for unique Telegram channels
SELECT DISTINCT
    MD5(channel_name) AS channel_id,  -- Generate unique channel_id using MD5 hash
    channel_name  -- Channel name from staging
FROM staging.stg_telegram_messages
WHERE
    channel_name != 'unknown'  -- Exclude placeholder 'unknown' values