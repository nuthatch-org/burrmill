-- expect: Timeout
-- tries: values built per row over nine million rows, summed
-- gap: a plan over in-memory sources with no join polls its whole input at once, so the deadline is seen only at its end
SELECT sum(length(repeat(CAST(range AS VARCHAR), 300))) AS n FROM range(9000000)
