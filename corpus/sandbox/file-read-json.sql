-- expect: NotAllowed
-- tries: read a file as JSON
SELECT * FROM read_json('/etc/hosts')
