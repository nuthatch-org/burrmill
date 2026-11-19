-- expect: LimitExceeded
-- tries: a cross join of nine million by nine million, filtered so no statistic answers it; its collected side is charged
SELECT count(*) AS n FROM range(9000000) a, range(9000000) b WHERE a.range <> b.range
