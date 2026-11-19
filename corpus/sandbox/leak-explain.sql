-- expect: NotAllowed
-- says: EXPLAIN
-- tries: #187: EXPLAIN, whose plan names segment paths and hidden registrations
EXPLAIN SELECT * FROM token__transfer
