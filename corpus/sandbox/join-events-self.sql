-- expect: Timeout | LimitExceeded
-- tries: a sealed table of a million rows joined to itself on a key of a hundred values, 10^10 rows
-- pending: #350, a DataFusion join does not yield inside itself
SELECT count(*) AS n FROM events a JOIN events b ON a.who = b.who
