-- expect: Timeout | LimitExceeded
-- tries: a nine million row equi-join with a filter, built in one poll
SELECT count(*) AS n FROM range(9000000) a JOIN range(9000000) b ON a.range = b.range AND a.range + b.range > 5
