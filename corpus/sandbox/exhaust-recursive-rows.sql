-- expect: Timeout
-- tries: a recursive CTE whose rows double each step
WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t, (VALUES (1), (2)) v(x) WHERE n < 64) SELECT count(*) AS c FROM t
