-- expect: Timeout | LimitExceeded
-- tries: three sealed scans joined on a computed key
-- pending: #350, a DataFusion join does not yield inside itself
SELECT count(*) AS n FROM events a JOIN events b ON a.block_number % 1000 = b.block_number % 1000 JOIN events c ON b.block_number % 1000 = c.block_number % 1000
