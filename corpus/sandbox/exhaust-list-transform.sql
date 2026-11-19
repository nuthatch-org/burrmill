-- expect: LimitExceeded
-- tries: #190: a list of values each under the limit, adding up without bound
SELECT list_transform(range(1000), x -> repeat('a', 10000000)) AS l
