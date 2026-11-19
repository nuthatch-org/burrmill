-- expect: NotAllowed
-- tries: sniff_csv, DuckDB's CVE-2024-41672, which read files with external access off
SELECT * FROM sniff_csv('/etc/hosts')
