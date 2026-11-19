-- expect: LimitExceeded
-- tries: values each under the value limit, summing past the pool
SELECT max(length(repeat(CAST(range AS VARCHAR), 1000000))) AS n FROM range(100000)
