-- expect: NotAllowed
-- says: not a table this surface serves
-- tries: a hidden name qualified with catalogue and schema
SELECT * FROM datafusion.public.token__transfer__raw
