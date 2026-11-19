-- expect: NotAllowed
-- says: CREATE EXTERNAL TABLE
-- tries: DataFusion's CREATE EXTERNAL TABLE over /etc/passwd
CREATE EXTERNAL TABLE pw STORED AS CSV LOCATION '/etc/passwd'
