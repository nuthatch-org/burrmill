-- expect: LimitExceeded
-- tries: a format width of two billion
SELECT printf('%2000000000d', 1) AS s
