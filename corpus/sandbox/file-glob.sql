-- expect: NotAllowed
-- tries: list a directory with glob
SELECT * FROM glob('/etc/*')
