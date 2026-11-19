-- expect: NoSegments
-- says: does not exist
-- tries: real Parquet files by glob as a table
SELECT * FROM '../../corpus/receipts/segments/*.parquet'
