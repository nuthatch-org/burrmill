-- expect: NoRows
-- tries: list the hidden registrations from information_schema
SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%\_\_raw' ESCAPE '\' OR table_name LIKE '%\_\_hot' ESCAPE '\' OR table_name LIKE '%\_\_union' ESCAPE '\'
