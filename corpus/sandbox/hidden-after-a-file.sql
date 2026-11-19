-- expect: NotAllowed
-- says: not a table this surface serves
-- tries: #189: a path as a table first, then a hidden name
SELECT * FROM (SELECT * FROM 'x.parquet') p, token__transfer__raw
