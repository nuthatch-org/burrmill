-- expect: Substrate
-- tries: a failed cast quoting a megabyte of its input
SELECT CAST(repeat('x', 1000000) AS INTEGER) AS n
