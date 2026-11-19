-- expect: Timeout
-- tries: values built per row over nine million rows, summed
SELECT sum(length(repeat(CAST(range AS VARCHAR), 300))) AS n FROM range(9000000)
