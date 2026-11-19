-- expect: NoSegments
-- says: does not exist
-- tries: a readable CSV in the repository as a table, so a refusal is not only a missing file
SELECT * FROM '../../corpus/ingest/canary.csv'
