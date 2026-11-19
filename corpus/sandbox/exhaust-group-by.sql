-- expect: LimitExceeded | Timeout
-- tries: two million groups of wide keys
SELECT count(*) AS c FROM (SELECT k, count(*) AS n FROM (SELECT repeat(CAST(range AS VARCHAR), 200) AS k FROM range(2000000)) GROUP BY k)
