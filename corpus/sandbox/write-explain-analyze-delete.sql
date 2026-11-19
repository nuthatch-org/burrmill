-- expect: NotAllowed
-- says: DELETE
-- tries: #13: a DELETE under EXPLAIN ANALYZE
EXPLAIN ANALYZE DELETE FROM labels
