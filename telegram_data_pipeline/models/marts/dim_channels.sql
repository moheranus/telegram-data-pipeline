{{ config(
    materialized='table',
    schema='marts'
) }}

-- Create dimension table for unique Telegram channels
SELECT DISTINCT
    MD5(channel_name) AS channel_id,
    channel_name
FROM {{ ref('stg_telegram_messages') }}
WHERE channel_name IN ('tenamereja', 'chemed')