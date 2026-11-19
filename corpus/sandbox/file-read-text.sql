-- expect: NotAllowed
-- tries: read a file whole as text
SELECT content FROM read_text('/etc/hosts')
