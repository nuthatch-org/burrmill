-- expect: LimitExceeded | Timeout
-- tries: a window over its whole input of wide rows
SELECT max(l) AS m FROM (SELECT last_value(repeat(CAST(range AS VARCHAR), 200)) OVER (ORDER BY range ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS l FROM range(2000000))
