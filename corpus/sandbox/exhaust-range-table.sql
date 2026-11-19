-- expect: Plan
-- says: ten million
-- tries: a table function of a trillion rows, built while planning
SELECT count(*) AS n FROM range(1000000000000)
