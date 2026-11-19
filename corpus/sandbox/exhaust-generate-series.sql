-- expect: Plan
-- says: ten million
-- tries: generate_series of a trillion rows
SELECT count(*) AS n FROM generate_series(1, 1000000000000)
