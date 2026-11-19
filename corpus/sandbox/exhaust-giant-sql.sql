-- expect: NotAllowed
-- says: longer than
-- tries: #185: a statement over 1 MiB, before any text rewrite runs
-- pad-to: 1048577
SELECT 1 AS one
