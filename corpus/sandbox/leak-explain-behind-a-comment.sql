-- expect: NotAllowed
-- says: EXPLAIN
-- tries: EXPLAIN behind a comment, past a check of the first word
/* a note */ EXPLAIN SELECT 1
