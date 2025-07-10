-- tests/valid_message_dates.sql
SELECT *
FROM staging.stg_telegram_messages
WHERE message_date < '2020-01-01' OR message_date > '2025-12-31'