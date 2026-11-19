-- expect: NotAllowed
-- says: not a table this surface serves
-- tries: a hidden name under a window the walk might skip
SELECT last_value(value IGNORE NULLS) OVER () AS x FROM token__transfer__raw
