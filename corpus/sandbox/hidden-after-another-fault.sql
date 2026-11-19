-- expect: NotAllowed
-- says: not a table this surface serves
-- tries: #189: a different fault first, so the hidden-name check is skipped
WITH "a b" AS (SELECT 1 AS k) SELECT * FROM "a b", token__transfer__raw
