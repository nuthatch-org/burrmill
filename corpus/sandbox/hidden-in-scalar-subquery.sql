-- expect: NotAllowed
-- says: not a table this surface serves
-- tries: a hidden name inside a scalar subquery
SELECT (SELECT count(*) FROM token__transfer__raw) AS n
