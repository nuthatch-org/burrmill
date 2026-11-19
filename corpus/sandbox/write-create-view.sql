-- expect: NotAllowed
-- tries: replace a served name with a view
CREATE OR REPLACE VIEW labels AS SELECT '0x1' AS a, 'mallory' AS l
