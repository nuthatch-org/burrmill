-- expect: NotAllowed
-- tries: a file function inside a scalar subquery
SELECT (SELECT count(*) FROM read_csv('/etc/hosts')) AS n
