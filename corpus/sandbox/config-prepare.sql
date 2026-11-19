-- expect: NotAllowed
-- tries: PREPARE a statement that outlives the request
PREPARE p AS SELECT * FROM labels
