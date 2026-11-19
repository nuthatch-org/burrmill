-- expect: NotAllowed
-- says: not a table this surface serves
-- tries: #10: sealed and hot rows unwindowed, as <table>__union
SELECT max(value) AS m FROM token__transfer__union
