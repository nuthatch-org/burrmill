-- expect: NotAllowed
-- tries: create a table later statements would see
CREATE TABLE kept AS SELECT 1 AS a
