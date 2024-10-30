SELECT
	userId,
	sessionId,
	channel
FROM {{ source('raw_data', 'user_session_channel') }}
WHERE sessionID IS NOT NULL
