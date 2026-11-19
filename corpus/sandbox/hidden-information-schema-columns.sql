-- expect: NoRows
-- tries: the hidden registration's columns from information_schema
SELECT column_name FROM information_schema.columns WHERE table_name = 'token__transfer__raw'
