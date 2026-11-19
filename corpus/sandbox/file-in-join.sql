-- expect: NotAllowed
-- tries: a file function joined to a served table
SELECT * FROM token__transfer t JOIN read_csv('/etc/hosts') h ON true
