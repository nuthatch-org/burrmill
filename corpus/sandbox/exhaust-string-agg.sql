-- expect: LimitExceeded
-- tries: string_agg growing one value past the limit
SELECT length(string_agg(repeat('a', 1000000), '')) AS n FROM range(100)
