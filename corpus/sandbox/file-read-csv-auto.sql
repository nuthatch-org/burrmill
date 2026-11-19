-- expect: NotAllowed
-- tries: read /etc/passwd with read_csv_auto
SELECT * FROM read_csv_auto('/etc/passwd')
