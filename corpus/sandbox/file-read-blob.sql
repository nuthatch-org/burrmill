-- expect: NotAllowed
-- tries: read a file whole as bytes
SELECT content FROM read_blob('/etc/hosts')
