-- expect: LimitExceeded
-- tries: a recursive CTE whose value doubles each step
WITH RECURSIVE t(n, s) AS (SELECT 1, 'a' UNION ALL SELECT n + 1, s || s FROM t WHERE n < 100) SELECT max(length(s)) AS m FROM t
