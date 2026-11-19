-- expect: Timeout | LimitExceeded
-- tries: a sealed table of a million rows joined to itself on a key of a hundred values, 10^10 rows
SELECT count(*) AS n FROM events a JOIN events b ON a.who = b.who
