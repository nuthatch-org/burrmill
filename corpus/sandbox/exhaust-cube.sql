-- expect: NotAllowed
-- tries: GROUP BY CUBE of 18 keys, 262,144 grouping sets expanded at plan time
SELECT count(*) AS n FROM (SELECT 0 AS c0, 1 AS c1, 2 AS c2, 3 AS c3, 4 AS c4, 5 AS c5, 6 AS c6, 7 AS c7, 8 AS c8, 9 AS c9, 10 AS c10, 11 AS c11, 12 AS c12, 13 AS c13, 14 AS c14, 15 AS c15, 16 AS c16, 17 AS c17) AS t GROUP BY CUBE(c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17)
