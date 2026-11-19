-- expect: NotAllowed
-- says: INSERT
-- tries: #13: EXPLAIN ANALYZE runs what it wraps, an INSERT included
EXPLAIN ANALYZE INSERT INTO labels VALUES ('0xz', 'mallory')
