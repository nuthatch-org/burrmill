-- expect: Timeout
-- tries: four ranges crossed, 10^12 rows
SELECT count(*) AS n FROM range(1000) a, range(1000) b, range(1000) c, range(1000) d WHERE a.range <> d.range
