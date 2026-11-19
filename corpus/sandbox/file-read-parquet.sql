-- expect: NotAllowed
-- tries: read the nest's own segments by path, past the catalogue
SELECT * FROM read_parquet('segments/*.parquet')
