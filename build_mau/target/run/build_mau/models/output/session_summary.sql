
  create or replace   view hw6.analytics.session_summary
  
   as (
    WITH u AS (
	SELECT * FROM hw6.analytics.user_session_channel
), st AS (
	SELECT * FROM hw6.analytics.session_timestamp
)
SELECT u.userId, u.sessionId, u.channel, st.ts
FROM u
JOIN st ON u.sessionID = st.sessionId
  );

