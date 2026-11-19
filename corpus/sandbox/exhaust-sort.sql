-- expect: LimitExceeded | Timeout
-- tries: a sort of two million wide rows
SELECT repeat(CAST(range AS VARCHAR), 200) AS k FROM range(2000000) ORDER BY k DESC
