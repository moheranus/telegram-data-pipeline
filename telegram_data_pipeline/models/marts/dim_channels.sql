{{ config(
    materialized='table',
    schema='marts'
) }}

SELECT DISTINCT
    channel_name,
    MD5(channel_name) AS channel_id
FROM staging.stg_telegram_messages
WHERE channel_name IS NOT NULL
