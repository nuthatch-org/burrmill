-- expect: NotAllowed
-- tries: read a bucket the process has credentials for
SELECT * FROM read_parquet('s3://bucket/x.parquet')
