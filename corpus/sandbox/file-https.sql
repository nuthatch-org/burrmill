-- expect: NoSegments
-- says: does not exist
-- tries: fetch over HTTPS as a table
SELECT * FROM 'https://example.com/x.parquet'
