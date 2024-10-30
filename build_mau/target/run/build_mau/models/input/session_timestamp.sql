
  create or replace   view hw6.analytics.session_timestamp
  
   as (
    SELECT
	sessionId,
	ts
FROM hw6.raw_data.session_timestamp
WHERE sessionId IS NOT NULL
  );

