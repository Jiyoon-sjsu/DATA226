SELECT
	userId,
	sessionId,
	channel
FROM hw6.raw_data.user_session_channel
WHERE sessionID IS NOT NULL