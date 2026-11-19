-- expect: LimitExceeded
-- tries: a list of a billion integers from the scalar range
SELECT range(1000000000) AS l
