-- tests/non_negative_message_length.sql
-- Check that message_length is non-negative in fct_messages
SELECT *
FROM {{ ref('fct_messages') }}
WHERE message_length < 0