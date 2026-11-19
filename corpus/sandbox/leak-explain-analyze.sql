-- expect: NotAllowed
-- says: EXPLAIN
-- tries: #187: EXPLAIN ANALYZE, which also runs the statement
EXPLAIN ANALYZE SELECT count(*) FROM token__transfer
