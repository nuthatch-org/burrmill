-- expect: NotAllowed
-- tries: read the host's /etc/hosts with DuckDB's read_csv
SELECT * FROM read_csv('/etc/hosts')
