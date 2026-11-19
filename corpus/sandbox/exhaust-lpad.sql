-- expect: LimitExceeded
-- tries: padding to two billion characters
SELECT lpad('a', 2000000000, 'b') AS s
