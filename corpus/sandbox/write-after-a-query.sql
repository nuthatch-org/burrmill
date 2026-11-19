-- expect: NotAllowed
-- says: statements where one is allowed
-- tries: a DELETE behind a query, hoping the first runs alone
SELECT a FROM labels; DELETE FROM labels
