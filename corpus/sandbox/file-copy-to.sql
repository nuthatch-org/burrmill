-- expect: NotAllowed
-- says: COPY
-- tries: write a file with COPY ... TO
COPY (SELECT 1 AS a) TO '/tmp/burrmill-sandbox-out.csv'
