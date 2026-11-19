-- expect: NoSegments
-- says: does not exist
-- tries: a quoted path as a table, which an unconfigured DataFusion session reads
SELECT * FROM '/etc/hosts'
