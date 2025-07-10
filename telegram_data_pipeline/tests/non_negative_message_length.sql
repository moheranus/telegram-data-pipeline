SELECT *
FROM {{ ref('fct_messages') }}
WHERE message_length < 0
