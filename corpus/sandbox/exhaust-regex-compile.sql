-- expect: Substrate
-- tries: a pattern whose compiled program is too big
SELECT regexp_like('a', 'a{1000}{1000}{1000}') AS m
