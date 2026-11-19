-- expect: NotAllowed
-- says: not a table this surface serves
-- tries: a hidden name under EXPLAIN
EXPLAIN SELECT * FROM token__transfer__raw
