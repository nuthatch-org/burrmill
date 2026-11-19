-- expect: NotAllowed
-- says: INSERT
-- tries: insert into a host's in-memory table
INSERT INTO labels VALUES ('0xz', 'mallory')
