-- expect: Substrate
-- says: bytes cut
-- tries: #190: an error quoting a whole 100 KB input back
SELECT json_extract(repeat('{', 100000), '$.a') AS j
