-- expect: Timeout
-- tries: a recursive CTE with no useful bound
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 1000000000000) SELECT count(*) AS n FROM t
