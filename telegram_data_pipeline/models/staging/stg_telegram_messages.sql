{{ config(
    materialized='table',
    schema='staging'
) }}

-- Stage raw Telegram messages, cleaning and standardizing data
WITH raw_data AS (
    SELECT
        message_id,
        LOWER(COALESCE(channel_name, 'unknown')) AS channel_name,
        CAST(COALESCE(message_date, '2020-01-01') AS TIMESTAMP) AS message_date,
        COALESCE(text, '') AS text,
        COALESCE(has_image, FALSE) AS has_image,
        sender_id,
        COALESCE(image_path, '') AS image_path,
        CAST(COALESCE(scraped_date, CURRENT_DATE) AS DATE) AS scraped_date
    FROM {{ source('raw', 'telegram_messages') }}
    WHERE
        message_id IS NOT NULL
        AND channel_name IS NOT NULL
        AND message_date IS NOT NULL
)
SELECT *
FROM raw_data
WHERE channel_name IN ('tenamereja', 'chemed')
-- Log unexpected channel names for debugging
{% if execute %}
    {% set unexpected_channels = run_query("SELECT DISTINCT channel_name FROM " ~ source('raw', 'telegram_messages') ~ " WHERE LOWER(channel_name) NOT IN ('tenamereja', 'chemed')") %}
    {% if unexpected_channels | length > 0 %}
        {{ log("Unexpected channel names found: " ~ unexpected_channels | map(attribute='channel_name') | list, info=True) }}
    {% endif %}
{% endif %}