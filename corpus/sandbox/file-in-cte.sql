-- expect: NoSegments
-- says: does not exist
-- tries: a path as a table inside a CTE
WITH h AS (SELECT * FROM '/etc/hosts') SELECT * FROM h
