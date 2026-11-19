-- expect: LimitExceeded
-- tries: unnest a list built to explode
SELECT count(*) AS n FROM (SELECT unnest(range(1000000000)) AS x)
