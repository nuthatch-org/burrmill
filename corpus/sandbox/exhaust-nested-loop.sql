-- expect: LimitExceeded
-- tries: a join on a predicate no hash or range join can take
SELECT count(*) AS n FROM range(1000000) a, range(1000000) b WHERE a.range + b.range = -1
