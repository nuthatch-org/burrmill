-- expect: Timeout
-- tries: a pattern over nine million rows of text, with no join and no scan of a segment
-- gap: a plan over in-memory sources with no join polls its whole input at once, so the deadline is seen only at its end
SELECT count(*) AS n FROM range(9000000) WHERE regexp_like(repeat(CAST(range AS VARCHAR), 50), '(\w+\s?)*$')
