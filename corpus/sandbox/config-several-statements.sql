-- expect: NotAllowed
-- says: statements where one is allowed
-- tries: a query, then a setting, in one request
SELECT 1 AS a; SET datafusion.execution.batch_size = 1
