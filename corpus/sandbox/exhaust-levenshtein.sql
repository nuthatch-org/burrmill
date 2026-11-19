-- expect: LimitExceeded
-- says: levenshtein over
-- tries: #184: levenshtein over two 100 KB strings, quadratic inside one poll
SELECT levenshtein(repeat('a', 100000), repeat('b', 100000)) AS n
