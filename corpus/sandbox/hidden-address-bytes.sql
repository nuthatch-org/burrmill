-- expect: Plan
-- says: address_bytes
-- tries: #188: address_bytes, whose refusal once turned binary addresses off for every caller
SELECT address_bytes('zz') AS b
