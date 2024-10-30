SELECT
	sessionId,
	ts
FROM hw6.raw_data.session_timestamp
WHERE sessionId IS NOT NULL